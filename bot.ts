import "dotenv/config";
import {
  Connection,
  Keypair,
  VersionedTransaction,
  PublicKey,
  SystemProgram,
  Transaction,
  TransactionMessage,
  ComputeBudgetProgram,
  type SimulatedTransactionResponse,
} from "@solana/web3.js";
import type { Logs } from "@solana/web3.js";
import { Program, AnchorProvider, BorshEventCoder } from "@coral-xyz/anchor";
import { getMint, getAssociatedTokenAddress, getAssociatedTokenAddressSync, TOKEN_2022_PROGRAM_ID, TOKEN_PROGRAM_ID } from "@solana/spl-token";
import { OnlinePumpSdk, PUMP_SDK, getBuyTokenAmountFromSolAmount, bondingCurvePda } from "@pump-fun/pump-sdk";
import { OnlinePumpAmmSdk, PUMP_AMM_PROGRAM_ID, PUMP_AMM_SDK, canonicalPumpPoolPda } from "@pump-fun/pump-swap-sdk";
import BN from "bn.js";
import fs from "fs";
import fetch from "node-fetch";
import bs58 from "bs58";
import https from "https";
import express from "express";
import { PRE_TRADE_MAX_MARKET_CAP_USD, orderBuyBuilders, shouldRejectPreTradeMarketCap } from "./hot-path.js";

export interface GrpcTradeEvent {
  type: "buy" | "sell";
  mint: string;
  user: string;
  solAmount: number;
  tokenAmount: number;
  signature: string;
  slot: number;
  timestamp: number;
}

/* ================= CONFIG ================= */

function requireEnv(name: string): string {
  const val = process.env[name];
  if (!val) throw new Error(`Missing required env var: ${name}`);
  return val;
}

const RPC_URL = requireEnv("RPC_URL");

// High-performance HTTP agent for all outbound requests (Jupiter, PumpPortal, Jito, etc.)
const agent = new https.Agent({
  keepAlive: true,
  maxSockets: 256,                    // Increased from 64 for high-concurrency bursts
  maxFreeSockets: 128,
  timeout: 8000,                      // 8s total timeout
  keepAliveMsecs: 1000,
  scheduling: 'fifo' as any,
});

// Separate agent for PumpPortal trade-local (can burst independently)
const pumpPortalAgent = new https.Agent({
  keepAlive: true,
  maxSockets: 64,
  timeout: 6000,
});

function makeConnection(url: string, opts: { disableWs?: boolean } = {}): Connection {
  if (!url || typeof url !== "string") {
    throw new Error(`Invalid URL: "${url}"`);
  }
  const trimmed = url.trim();
  if (!trimmed.startsWith("http://") && !trimmed.startsWith("https://")) {
    throw new Error(`Endpoint URL must start with http: or https:: "${trimmed}"`);
  }
  // Decide whether to disable websockets for this connection.
  // Priority: explicit opts.disableWs takes precedence. Otherwise, honor
  // DISABLE_HELIUS_WS env when set to "true" and the URL looks like Helius.
  const disableHeliusWsEnv = (process.env.DISABLE_HELIUS_WS ?? "false").toLowerCase() === "true";
  const looksLikeHelius = trimmed.includes("helius") || trimmed.includes("api.helius") || trimmed.includes("helius.dev");
  const finalDisableWs = Boolean(opts.disableWs) || (disableHeliusWsEnv && looksLikeHelius);
  const wsEndpoint = finalDisableWs ? ("" as any) : trimmed.replace("https", "wss");
  return new Connection(trimmed, {
    commitment: "processed",
    wsEndpoint,
    httpAgent: agent,
  });
}

// If PumpPortal is used as primary detection, disable Helius WS to avoid 429s
const connection = makeConnection(RPC_URL, { disableWs: (process.env.PUMPPORTAL ?? "true").toLowerCase() === "true" });

const WS_FANOUT = Math.max(1, Math.min(1, Number(process.env.WS_FANOUT ?? 1))); // Force 1 for Helius free tier
const EXTRA_WS_URLS = (process.env.EXTRA_WS_URLS ?? "")
  .split(",")
  .map((s) => s.trim())
  .filter((s) => s.startsWith("http://") || s.startsWith("https://"));

const DETECTION_URLS: string[] = [];
for (let i = 0; i < WS_FANOUT; i++) DETECTION_URLS.push(RPC_URL);
DETECTION_URLS.push(...EXTRA_WS_URLS);

// Deduplicate URLs to avoid multiple connections to same endpoint
const uniqueDetectionUrls = [...new Set(DETECTION_URLS)];
if (uniqueDetectionUrls.length !== DETECTION_URLS.length) {
  console.log(`⚠️ Deduplicated detection URLs: ${DETECTION_URLS.length} -> ${uniqueDetectionUrls.length}`);
}

// Short-term control: if true, only use PumpPortal for detection and skip
// websocket/grpc-based detection channels. Default is false (legacy behavior).
const ONLY_PUMPPORTAL_DETECTION = (process.env.ONLY_PUMPPORTAL_DETECTION ?? "false").toLowerCase() === "true";

const BROADCAST_RPC_URL = process.env.BROADCAST_RPC_URL ?? "https://api.mainnet-beta.solana.com";
const broadcastConnection = makeConnection(BROADCAST_RPC_URL);

const wallet = Keypair.fromSecretKey(bs58.decode(requireEnv("PRIVATE_KEY")));

// Multi-target wallet support: comma-separated list
const TARGET_WALLETS = (process.env.TARGET_WALLETS ?? process.env.TARGET_WALLET ?? "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean)
  .map((s) => new PublicKey(s));

if (TARGET_WALLETS.length === 0) {
  throw new Error("Missing required env var: TARGET_WALLETS (or TARGET_WALLET)");
}

// Backwards compatibility: first wallet is primary
const TARGET_WALLET = TARGET_WALLETS[0]!;
const TARGET_WALLET_STRINGS = TARGET_WALLETS.map((w) => w.toString());
const TARGET_WALLET_SET = new Set(TARGET_WALLET_STRINGS);

function isTargetWalletKey(key: PublicKey): boolean {
  return TARGET_WALLETS.some((w) => w.equals(key));
}

function isTargetWalletAddress(address: string): boolean {
  return TARGET_WALLET_SET.has(address);
}

const BUY_AMOUNT_SOL = Number(process.env.BUY_AMOUNT_SOL);
const BUY_AMOUNT = BUY_AMOUNT_SOL * 1e9;
const BASE_PRIORITY_FEE = Number(process.env.PRIORITY_FEE_LAMPORTS ?? 500_000);
const FORCE_PRIORITY_FEE = Number(process.env.FORCE_PRIORITY_FEE_LAMPORTS ?? 0);
const SLIPPAGE_BPS = Number(process.env.SLIPPAGE_BPS ?? 3000);
const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS ?? 2500);

// Jito configuration
const JITO_BLOCK_ENGINE_URL = process.env.JITO_URL ?? "https://mainnet.block-engine.jito.wtf/api/v1/bundles";
const JITO_TIP_ACCOUNTS: string[] = [
  "9n3d1K5YD2vECAbRFhFFGYNNjiXtHXJWn9F31t89vsAV",
  "aTtUk2DHgLhKZRDjePq6eiHRKC1XXFMBiSUfQ2JNDbN",
  "B1mrQSpdeMU9gCvkJ6VsXVVoYjRGkNA7TtjMyqxrhecH",
  "9ttgPBBhRYFuQccdR1DSnb7hydsWANoDsV3P9kaGMCEh",
  "4xgEmT58RwTNsF5xm2RMYCnR1EVukdK8a1i2qFjnJFu3",
  "EoW3SUQap7ZeynXQ2QJ847aerhxbPVr843uMeTfc9dxM",
  "E2eSqe33tuhAHKTrwky5uEjaVqnb2T9ns6nHHUrN8588",
  "ARTtviJkLLt6cHGQDydfo1Wyk6M4VGZdKZ2ZhdnJL336",
];
const JITO_TIP_BPS = Number(process.env.JITO_TIP_BPS ?? 2000); // 20% of position

function randomTipAccount(): PublicKey {
  const idx = Math.floor(Math.random() * JITO_TIP_ACCOUNTS.length);
  const addr = JITO_TIP_ACCOUNTS[idx] ?? JITO_TIP_ACCOUNTS[0];
  return new PublicKey(addr as string);
}

// gRPC Detection (Shyft Yellowstone)
const GRPC_ENDPOINT = process.env.GRPC_ENDPOINT ?? "https://grpc.shyft.to";
const GRPC_TOKEN = process.env.GRPC_TOKEN;
const GRPC_ENABLED = !!GRPC_TOKEN;

const MAX_PRICE_IMPACT = Number(process.env.MAX_PRICE_IMPACT ?? 0.15);
const ONLY_DIRECT_ROUTES = (process.env.ONLY_DIRECT_ROUTES ?? "true").toLowerCase() === "true";
const PUMPPORTAL_ENABLED = (process.env.PUMPPORTAL ?? "true").toLowerCase() === "true";
const PREFER_PUMPPORTAL_ONLY = (process.env.PREFER_PUMPPORTAL_ONLY ?? "false").toLowerCase() === "true";

// Per-mint Pump-only fallback cache: when a mint returns Anchor error 6005
// (BondingCurveComplete / migrated to AMM), we mark it so the bot will
// skip PumpPortal attempts for that mint for a short TTL to avoid repeats.
const PUMP_FALLBACK_TTL_MS = Number(process.env.PUMP_FALLBACK_TTL_MS ?? 600000); // 10m default
const pumpFallbackCache = new Map<string, number>(); // mint -> expiresAt
const pumpFirstFailureLogged = new Set<string>();

function isPumpFallbackActive(mint: string): boolean {
  const exp = pumpFallbackCache.get(mint);
  if (!exp) return false;
  if (Date.now() > exp) {
    pumpFallbackCache.delete(mint);
    return false;
  }
  return true;
}

const MIN_WALLET_BALANCE_SOL = Number(process.env.MIN_WALLET_BALANCE_SOL ?? 0.02);

const SOL_MINT = "So11111111111111111111111111111111111111112";
const SOL_PRICE_CACHE_TTL_MS = 60_000;
let solPriceCache: { priceUsd: number; timestamp: number } | null = null;
let solPriceRefresh: Promise<number | null> | null = null;

function getConfiguredSolPriceUsd(): number | null {
  const value = Number(process.env.SOL_PRICE_USD ?? 0);
  return Number.isFinite(value) && value > 0 ? value : null;
}

async function refreshSolPriceUsd(): Promise<number | null> {
  if (solPriceRefresh) return solPriceRefresh;

  solPriceRefresh = (async () => {
    try {
      const response = await fetch(`https://api.jup.ag/price/v3?ids=${SOL_MINT}`, {
        agent,
        headers: process.env.JUP_API_KEY ? { "x-api-key": process.env.JUP_API_KEY } : undefined,
      });
      if (!response.ok) throw new Error(`Jupiter HTTP ${response.status}`);
      const payload: any = await response.json();
      const priceUsd = Number(payload?.[SOL_MINT]?.usdPrice ?? 0);
      if (!Number.isFinite(priceUsd) || priceUsd <= 0) throw new Error("Jupiter returned no positive SOL price");
      solPriceCache = { priceUsd, timestamp: Date.now() };
      console.log(`SOL price updated: $${priceUsd.toFixed(2)} (Jupiter)`);
      return priceUsd;
    } catch (jupiterError: any) {
      try {
        const response = await fetch("https://api.binance.com/api/v3/ticker/price?symbol=SOLUSDT", { agent });
        if (!response.ok) throw new Error(`Binance HTTP ${response.status}`);
        const payload: any = await response.json();
        const priceUsd = Number(payload?.price ?? 0);
        if (!Number.isFinite(priceUsd) || priceUsd <= 0) throw new Error("Binance returned no positive SOL price");
        solPriceCache = { priceUsd, timestamp: Date.now() };
        console.log(`SOL price updated: $${priceUsd.toFixed(2)} (Binance)`);
        return priceUsd;
      } catch (binanceError: any) {
        console.log(`SOL price provider failed: Jupiter=${jupiterError?.message ?? String(jupiterError)}; Binance=${binanceError?.message ?? String(binanceError)}`);
        try {
          const response = await fetch("https://api.coingecko.com/api/v3/simple/price?ids=solana&vs_currencies=usd", {
            agent,
            headers: {
              Accept: "application/json",
              "User-Agent": "sniper-bot/1.0 SOL-price-client",
            },
          });
          if (!response.ok) throw new Error(`CoinGecko HTTP ${response.status}`);
          const payload: any = await response.json();
          const priceUsd = Number(payload?.solana?.usd ?? 0);
          if (!Number.isFinite(priceUsd) || priceUsd <= 0) throw new Error("CoinGecko returned no positive SOL price");
          solPriceCache = { priceUsd, timestamp: Date.now() };
          console.log(`SOL price updated: $${priceUsd.toFixed(2)} (CoinGecko)`);
          return priceUsd;
        } catch (coinGeckoError: any) {
          console.log(`SOL price provider failed: CoinGecko=${coinGeckoError?.message ?? String(coinGeckoError)}`);
          return null;
        }
      }
    }
  })().finally(() => {
    solPriceRefresh = null;
  });

  return solPriceRefresh;
}

async function getSolPriceUsd(): Promise<number | null> {
  if (solPriceCache && Date.now() - solPriceCache.timestamp < SOL_PRICE_CACHE_TTL_MS) {
    return solPriceCache.priceUsd;
  }

  const livePrice = await refreshSolPriceUsd();
  if (livePrice !== null) return livePrice;

  const configuredPrice = getConfiguredSolPriceUsd();
  if (configuredPrice !== null) {
    console.log(`SOL price fallback: $${configuredPrice.toFixed(2)} (SOL_PRICE_USD)`);
  } else {
    console.log("SOL price unavailable: live providers failed and SOL_PRICE_USD is not set");
  }
  return configuredPrice;
}

const JUP_BASE = process.env.JUP_BASE_URL ?? "https://quote-api.jup.ag/v1";
const JUP_LEGACY_BASE = process.env.JUP_LEGACY_BASE_URL ?? "https://api.jup.ag/swap/v1";
const JUP_API_KEY = process.env.JUP_API_KEY;
const JUP_FALLBACK_TTL_MS = Number(process.env.JUP_FALLBACK_TTL_MS ?? 30_000);
let jupiterDisabledUntil = 0;
const jupiterNoRouteCache = new Map<string, number>();

function isJupiterAvailable(): boolean {
  return Date.now() >= jupiterDisabledUntil;
}

function disableJupiter(reason: string, durationMs = JUP_FALLBACK_TTL_MS) {
  jupiterDisabledUntil = Math.max(jupiterDisabledUntil, Date.now() + durationMs);
  console.log(`⚠️ Jupiter disabled for ${Math.round(durationMs / 1000)}s: ${reason}`);
}

function skipJupiterForMint(mint: string): boolean {
  const expiry = jupiterNoRouteCache.get(mint);
  if (!expiry) return false;
  if (Date.now() > expiry) {
    jupiterNoRouteCache.delete(mint);
    return false;
  }
  return true;
}

// bloXroute (free tier)
const BLOXROUTE_AUTH = process.env.BLOXROUTE_AUTH;
const BLOXROUTE_ENABLED = !!BLOXROUTE_AUTH;
const BLOXROUTE_ENDPOINT = "https://api.bloxroute.com/solana/v1/tx/submit";

/* ================= CIRCUIT BREAKER & DRY-RUN ================= */
const CIRCUIT_BREAKER_MAX_LOSSES = Number(process.env.CIRCUIT_BREAKER_MAX_LOSSES ?? 3);
const CIRCUIT_BREAKER_WINDOW_MS = Number(process.env.CIRCUIT_BREAKER_WINDOW_MS ?? 15 * 60 * 1000); // 15 min
const CIRCUIT_BREAKER_COOLDOWN_MS = Number(process.env.CIRCUIT_BREAKER_COOLDOWN_MS ?? 30 * 60 * 1000); // 30 min
let recentStopLosses: number[] = [];
let circuitBreakerUntil = 0;

function recordStopLoss() {
  const now = Date.now();
  recentStopLosses = recentStopLosses.filter(t => now - t < CIRCUIT_BREAKER_WINDOW_MS);
  recentStopLosses.push(now);
  if (recentStopLosses.length >= CIRCUIT_BREAKER_MAX_LOSSES) {
    circuitBreakerUntil = now + CIRCUIT_BREAKER_COOLDOWN_MS;
    console.log(`\u{1F6AB} CIRCUIT BREAKER TRIGGERED — pausing new buys for ${CIRCUIT_BREAKER_COOLDOWN_MS / 60000} min (${recentStopLosses.length} stop-losses in window)`);
    sendTelegramAlert(`\u{1F6AB} <b>CIRCUIT BREAKER</b>\n${recentStopLosses.length} stop-losses in ${CIRCUIT_BREAKER_WINDOW_MS / 60000} min\nPausing buys for ${CIRCUIT_BREAKER_COOLDOWN_MS / 60000} min`);
  }
}

function isCircuitBreakerActive(): boolean {
  return Date.now() < circuitBreakerUntil;
}

function getCircuitBreakerRemainingMs(): number {
  return Math.max(0, circuitBreakerUntil - Date.now());
}

const DRY_RUN = (process.env.DRY_RUN ?? "false") === "true";
const DRY_RUN_LOG_INTERVAL_MS = Number(process.env.DRY_RUN_LOG_INTERVAL_MS ?? 60_000);
let dryRunStats = { buys: 0, sells: 0, totalLatencyMs: 0, pnlSol: 0 };
if (DRY_RUN) {
  console.log("\u{1F9EA} DRY-RUN MODE ENABLED — no real transactions will be sent");
  setInterval(() => {
    if (dryRunStats.buys > 0) {
      console.log(`\u{1F7E2} DRY-RUN STATS: buys=${dryRunStats.buys}, avgLatency=${(dryRunStats.totalLatencyMs / dryRunStats.buys).toFixed(0)}ms, estPnL=${dryRunStats.pnlSol.toFixed(4)} SOL`);
    }
  }, DRY_RUN_LOG_INTERVAL_MS);
}

const PUMPFUN_PROGRAM = new PublicKey("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P");
const pumpSdk = new OnlinePumpSdk(connection);
const LOCAL_PUMP_COMPUTE_UNITS = 200_000;

/* ================= PUMPPORTAL PRIMARY DETECTION ================= */
// PumpPortal WS delivers pre-decoded trade data (mint, buy/sell, SOL amount)
// for the target wallet with NO RPC round-trip. This is the FASTEST detection
// channel (~100ms typical). Requires API key + linked wallet funded with >=0.02 SOL.
// Bonding-curve data is NOT free despite docs implying otherwise -- confirmed via testing.

/* ================= BONDING CURVE CACHE ================= */
// Cache bonding curve state from PumpPortal detection to eliminate "zero tokens"
// / 6004 errors on brand-new tokens. PumpPortal emits the bonding curve address
// in its trade event, so we can pre-fetch & cache it before the buy fires.
const BONDING_CURVE_TTL_MS = 5 * 60 * 1000; // 5 min

interface CachedBondingCurve {
  bondingCurve: PublicKey;
  associatedBondingCurve: PublicKey;
  virtualQuoteReserves: BN;
  virtualTokenReserves: BN;
  realQuoteReserves: BN;
  realTokenReserves: BN;
  tokenTotalSupply?: BN | null;
  timestamp: number;
}

const bondingCurveCache = new Map<string, CachedBondingCurve>();

async function primeMintDerivedState(mint: string, detectionTs: number = nowMs()): Promise<boolean> {
  const now = nowMs();
  const cached = bondingCurveCache.get(mint);
  const warm = buyStateWarmCache.get(mint);
  const cacheFresh = Boolean(cached && warm && now - cached.timestamp < BONDING_CURVE_TTL_MS && now - warm.timestamp < 30_000);
  if (cacheFresh) return true;

  const mintKey = new PublicKey(mint);
  const bondingCurveKey = bondingCurvePda(mintKey);
  const primeStart = nowMs();
  console.log(`PRIME_START: mint=${mint} ts=${Date.now()} rel=${primeStart - detectionTs}ms source=mint-derived`);

  let tokenProgram = TOKEN_PROGRAM_ID;
  let mintInfo: any = null;
  try {
    mintInfo = await pumpSdkConnection.getAccountInfo(mintKey, "processed");
    if (mintInfo?.owner) {
      tokenProgram = mintInfo.owner;
    }
  } catch (e: any) {
    console.log(`PRIME_OP_FAILED: mintInfo.getAccountInfo dur=${nowMs() - primeStart}ms err=${e?.message ?? String(e)}`);
  }

  const associatedBondingCurveKey = getAssociatedTokenAddressSync(mintKey, bondingCurveKey, true, tokenProgram);

  const [bcAccount, abcAccount, feeConfig, global, mintState, buyState] = await Promise.all([
    pumpSdkConnection.getAccountInfo(bondingCurveKey, "processed"),
    pumpSdkConnection.getAccountInfo(associatedBondingCurveKey, "processed"),
    warm?.feeConfig && now - warm.timestamp < 60_000 ? Promise.resolve(warm.feeConfig) : pumpSdkForBuild.fetchFeeConfig(),
    warm?.global && now - warm.timestamp < 60_000 ? Promise.resolve(warm.global) : pumpSdkForBuild.fetchGlobal(),
    warm?.mintState && now - warm.timestamp < 60_000 ? Promise.resolve(warm.mintState) : getMint(pumpSdkConnection, mintKey, "processed", tokenProgram),
    warm?.buyState && now - warm.timestamp < 60_000 ? Promise.resolve(warm.buyState) : pumpSdkForBuild.fetchBuyState(mintKey, wallet.publicKey, tokenProgram),
  ]);

  let populated = false;
  try {
    if (bcAccount?.data && bcAccount.data.length >= 48) {
      const data = bcAccount.data;
      const virtualTokenReserves = new BN(data.subarray(8, 16).readBigUInt64LE(0).toString());
      const virtualQuoteReserves = new BN(data.subarray(16, 24).readBigUInt64LE(0).toString());
      const realTokenReserves = new BN(data.subarray(24, 32).readBigUInt64LE(0).toString());
      const realQuoteReserves = new BN(data.subarray(32, 40).readBigUInt64LE(0).toString());
      const tokenTotalSupply = new BN(data.subarray(40, 48).readBigUInt64LE(0).toString());
      bondingCurveCache.set(mint, {
        bondingCurve: bondingCurveKey,
        associatedBondingCurve: associatedBondingCurveKey,
        virtualTokenReserves,
        virtualQuoteReserves,
        realTokenReserves,
        realQuoteReserves,
        tokenTotalSupply,
        timestamp: Date.now(),
      });
      populated = true;
    }
  } catch (e: any) {
    console.log(`PRIME_FAILED: mint=${mint} dur=${nowMs() - primeStart}ms err=${e?.message ?? String(e)}`);
    return false;
  }

  if (buyState && mintState && feeConfig && global) {
    buyStateWarmCache.set(mint, { timestamp: Date.now(), buyState, mintState, feeConfig, global });
  }

  const primeEnd = nowMs();
  console.log(`PRIME_COMPLETE: mint=${mint} totalDur=${primeEnd - primeStart}ms populated=${populated} source=mint-derived buyStateWarmCache=${buyState ? "present" : "absent"} feeConfig=${feeConfig ? "present" : "absent"} global=${global ? "present" : "absent"}`);
  return populated;
}

// Pump.fun global PDA (constant, never changes)
const PUMPFUN_GLOBAL_PDA = new PublicKey("4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf");

/* ================= PRE-BUILD CACHE (Top Pump Tokens) ================= */
// Proactively fetch bonding curve data for top 500 Pump.fun tokens by volume
// so local SDK build is instant when target wallet buys them.
const PREBUILD_CACHE_TTL_MS = 10 * 60 * 1000; // 10 min
const PREBUILD_MAX_TOKENS = 500;
const prebuildCache = new Map<string, CachedBondingCurve>();
const buyStateWarmCache = new Map<string, { timestamp: number; buyState: any; mintState: any; feeConfig: any; global: any }>();

async function refreshPrebuildCache() {
  try {
    // Fetch top tokens from Pump.fun API (graduated + high volume)
    const res = await fetch("https://frontend-api.pump.fun/coins?offset=0&limit=500&sort=market_cap&include_nsfw=false", { agent });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const tokens = await res.json();
    const mints = tokens.map((t: any) => t.mint).filter((m: string) => m);

    console.log(`\u{1F4A8} Pre-building cache for ${mints.length} top Pump tokens...`);
    let cached = 0;
    for (const mint of mints) {
      try {
        const mintKey = new PublicKey(mint);
        const bondingCurveAddress = bondingCurvePda(mintKey);
        const [bcAccount, abcAccountPlaceholder, feeConfig] = await Promise.all([
          pumpSdkConnection.getAccountInfo(bondingCurveAddress, "processed"),
          pumpSdkConnection.getAccountInfo(new PublicKey("ASSOCIATED_BONDING_CURVE_PLACEHOLDER"), "processed"),
          pumpSdkForBuild.fetchFeeConfig(),
        ]);
        // Note: Associated bonding curve needs to be derived from mint
        const associatedBondingCurve = (
          await PublicKey.findProgramAddress(
            [Buffer.from("associated-bonding-curve"), mintKey.toBuffer()],
            PUMPFUN_PROGRAM
          )
        )[0];
        const abcAccount = await pumpSdkConnection.getAccountInfo(associatedBondingCurve, "processed");

        if (bcAccount?.data && abcAccount?.data && feeConfig) {
          const data = bcAccount.data;
          if (data.length >= 48) {
            // Anchor/SDK BondingCurve layout: discriminator (8) then
            // virtualTokenReserves (8), virtualQuoteReserves (8),
            // realTokenReserves (8), realQuoteReserves (8), tokenTotalSupply (8)
            prebuildCache.set(mint, {
              bondingCurve: bondingCurveAddress,
              associatedBondingCurve: associatedBondingCurve,
              virtualTokenReserves: new BN(data.subarray(8, 16).readBigUInt64LE(0).toString()),
              virtualQuoteReserves: new BN(data.subarray(16, 24).readBigUInt64LE(0).toString()),
              realTokenReserves: new BN(data.subarray(24, 32).readBigUInt64LE(0).toString()),
              realQuoteReserves: new BN(data.subarray(32, 40).readBigUInt64LE(0).toString()),
              tokenTotalSupply: data.length >= 48 ? new BN(data.subarray(40, 48).readBigUInt64LE(0).toString()) : null,
              timestamp: Date.now(),
            });
            cached++;
          }
        }
      } catch {
        // Ignore individual token failures
      }
    }
    console.log(`âœ… Pre-build cache ready: ${cached}/${mints.length} tokens`);
  } catch (e: any) {
    console.log("âš ï¸ Pre-build cache refresh failed:", e.message);
  }
}

// Refresh pre-build cache every 10 minutes
setInterval(refreshPrebuildCache, PREBUILD_CACHE_TTL_MS);
// Initial fetch
refreshPrebuildCache();

/* ================= DEDICATED PUMP SDK RPC ================= */
// Isolate SDK read traffic from detection/write traffic so free-tier Helius
// rate limits on detection WS don't starve the buy builder.
const PUMP_SDK_RPC_URL = process.env.PUMP_SDK_RPC_URL ?? "https://rpc.ankr.com/solana";
const pumpSdkConnection = makeConnection(PUMP_SDK_RPC_URL, {
  disableWs: (process.env.DISABLE_HELIUS_WS ?? "false").toLowerCase() === "true",
});
const pumpSdkForBuild = new OnlinePumpSdk(pumpSdkConnection);
const pumpAmmSdkForBuild = new OnlinePumpAmmSdk(pumpSdkConnection);

// Quick helper for readable timestamps
function nowMs() { return Date.now(); }

// Runtime check of Pump SDK RPC responsiveness so we can log an actionable
// hint when the dedicated RPC is slow/unindexed for Pump accounts.
async function verifyPumpSdkRpc(): Promise<void> {
  try {
    const started = nowMs();
    // getVersion is lightweight; getSlot approximates latency too
    const version = await pumpSdkConnection.getVersion();
    const slot = await pumpSdkConnection.getSlot("processed");
    const elapsed = nowMs() - started;
    console.log(`âœ… Pump SDK RPC OK: ${PUMP_SDK_RPC_URL} (rt=${elapsed}ms) version=${JSON.stringify(version)}`);

    if (elapsed > 400) {
      console.log(`⚠️ Pump SDK RPC appears slow (>400ms). Consider using a premium/indexed RPC for PUMP_SDK_RPC_URL`);
    }
  } catch (err: any) {
    console.log(`âš ï¸ Pump SDK RPC check failed: ${err?.message ?? String(err)} — PUMP_SDK_RPC_URL may be unreachable or not fully indexed`);
  }
}

// Kick off a non-blocking probe
verifyPumpSdkRpc().catch(() => {});

/* ================= DEDICATED BUY EXECUTION RPC ================= */
// Low-latency RPC for transaction submission only (no WS)
// Set BUY_EXEC_RPC_URL to a premium endpoint (Helius paid, Triton, QuickNode, etc.)
// Falls back to main RPC if not set.
const BUY_EXEC_RPC_URL = process.env.BUY_EXEC_RPC_URL ?? RPC_URL;
const buyExecConnection = makeConnection(BUY_EXEC_RPC_URL, { disableWs: true });


const originalLog = console.log;
console.log = (...args: any[]) => {
  const msg = args.join(' ');
  if (msg.includes('detection channel(s) rotated onto fresh websockets')) return;
  if (msg.includes('Polling cursor initialized at:')) return;
  if (msg.includes('Keep-alive server on port')) return;
  originalLog(...args);
};

/* ================= TELEGRAM ALERTS ================= */

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;
const TELEGRAM_EXTENDED = (process.env.TELEGRAM_EXTENDED ?? "false").toLowerCase() === "true";
const TELEGRAM_COOLDOWN_MS = Number(process.env.TELEGRAM_COOLDOWN_MS ?? 5000);
const TELEGRAM_COOLDOWN_NOTIFY_MS = Number(process.env.TELEGRAM_COOLDOWN_NOTIFY_MS ?? 60_000);
const TELEGRAM_WEBHOOK_SECRET = process.env.TELEGRAM_WEBHOOK_SECRET || "mysecret123";
const TELEGRAM_WEBHOOK_MODE = (process.env.TELEGRAM_WEBHOOK_MODE ?? "true").toLowerCase() !== "false" && !!TELEGRAM_BOT_TOKEN;
const TELEGRAM_WEBHOOK_ROUTE = `/telegram-webhook/${TELEGRAM_WEBHOOK_SECRET}`;
const TELEGRAM_WEBHOOK_URL = process.env.TELEGRAM_WEBHOOK_URL ||
  (process.env.RENDER_EXTERNAL_URL ? `${process.env.RENDER_EXTERNAL_URL.replace(/\/$/, "")}${TELEGRAM_WEBHOOK_ROUTE}` : `http://localhost:${process.env.PORT || 3000}${TELEGRAM_WEBHOOK_ROUTE}`);

// Rate-limiting state per chat to avoid command floods
const telegramLastCommandAt = new Map<string, number>();
const telegramLastNotifyAt = new Map<string, number>();

// Small cache for trade_journal to avoid repeated Upstash hits
let telegramJournalCache: { ts: number; journal: any[] } | null = null;
const TELEGRAM_JOURNAL_CACHE_TTL_MS = 5_000;

async function sendTelegramAlert(message: string) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;
  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
  try {
    await fetch(url, {
      agent,
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        chat_id: TELEGRAM_CHAT_ID,
        text: message,
        parse_mode: "HTML",
        disable_web_page_preview: true,
      }),
    });
  } catch (e: any) {
    console.log("Telegram alert failed:", e.message);
  }
}

async function sendTelegramMessage(chatId: number | string, text: string) {
  if (!TELEGRAM_BOT_TOKEN) return;
  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
  try {
    await fetch(url, {
      agent,
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        chat_id: chatId,
        text,
        parse_mode: "HTML",
      }),
    });
  } catch (e: any) {
    console.log("Telegram message failed:", e.message);
  }
}

async function setupTelegramWebhook() {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_WEBHOOK_MODE) return;
  try {
    const deleteUrl = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/deleteWebhook`;
    await fetch(deleteUrl, {
      agent,
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ drop_pending_updates: true }),
    }).catch(() => undefined);

    const setUrl = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/setWebhook`;
    const res = await fetch(setUrl, {
      agent,
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        url: TELEGRAM_WEBHOOK_URL,
        drop_pending_updates: true,
      }),
    });
    const data = await res.json().catch(() => ({}));
    console.log("Webhook setup result:", data);
  } catch (e: any) {
    console.log("Webhook setup failed:", e?.message ?? String(e));
  }
}

async function handleTelegramCommandMessage(msg: any) {
  if (!msg || String(msg.chat?.id) !== String(TELEGRAM_CHAT_ID)) return;

  const chatId = String(msg.chat?.id);
  const now = Date.now();
  const last = telegramLastCommandAt.get(chatId) ?? 0;
  if (now - last < TELEGRAM_COOLDOWN_MS) {
    const lastNotify = telegramLastNotifyAt.get(chatId) ?? 0;
    if (now - lastNotify > TELEGRAM_COOLDOWN_NOTIFY_MS) {
      telegramLastNotifyAt.set(chatId, now);
      try {
        await sendTelegramAlert(`Please wait ${Math.ceil((TELEGRAM_COOLDOWN_MS - (now - last)) / 1000)}s before sending another command.`);
      } catch {
        // ignore notify failures
      }
    }
    return;
  }

  const msgAgeSec = Math.floor(Date.now() / 1000) - (msg.date ?? 0);
  if (msgAgeSec > 30) {
    console.log(`⏩ Skipping stale Telegram update (${msgAgeSec}s old): ${msg.text}`);
    return;
  }

  const text = (msg.text ?? "").trim().toLowerCase();
  if (text === "/pause") {
    telegramLastCommandAt.set(chatId, Date.now());
    botPaused = true;
    console.log("⏸ Bot PAUSED via Telegram command");
    await sendTelegramAlert("⏸ <b>Bot paused.</b> No new buys will be taken. Existing positions still monitored/sold normally. Send /resume to continue.");
  } else if (text === "/resume") {
    telegramLastCommandAt.set(chatId, Date.now());
    botPaused = false;
    console.log("▶ Bot RESUMED via Telegram command");
    await sendTelegramAlert("▶ <b>Bot resumed.</b> New buys re-enabled.");
  } else if (text === "/status") {
    telegramLastCommandAt.set(chatId, Date.now());
    const balanceStr = lastKnownBalanceLamports !== null
      ? `${(lastKnownBalanceLamports / 1e9).toFixed(4)} SOL`
      : "unknown";
    await sendTelegramAlert(
      `📢 <b>Status</b>\n` +
      `Paused: ${botPaused ? "YES" : "no"}\n` +
      `Open positions: ${positions.size}\n` +
      `Wallet: ${balanceStr}\n` +
      `WS channels: ${DETECTION_URLS.length}\n` +
      `PumpPortal: ${pumpPortalConnected ? "✅ connected" : "❌ NOT connected"}`
    );
  } else if (text === "/positions" && TELEGRAM_EXTENDED) {
    telegramLastCommandAt.set(chatId, Date.now());
    await sendTelegramAlert(await getTelegramPositionsText());
  } else if (text === "/stats" && TELEGRAM_EXTENDED) {
    telegramLastCommandAt.set(chatId, Date.now());
    await sendTelegramAlert(await getTelegramStatsText());
  } else if (text === "/history" && TELEGRAM_EXTENDED) {
    telegramLastCommandAt.set(chatId, Date.now());
    await sendTelegramAlert(await getTelegramHistoryText());
  } else if (text === "/settings" && TELEGRAM_EXTENDED) {
    telegramLastCommandAt.set(chatId, Date.now());
    await sendTelegramAlert(await getTelegramSettingsText());
  }
}

// Lightweight, safe Telegram helper responses (non-blocking, cached-friendly)
async function getTelegramPositionsText(): Promise<string> {
  if (positions.size === 0) return "No open positions.";
  const lines: string[] = [];
  let i = 0;
  for (const [mint, pos] of positions.entries()) {
    if (i++ >= 20) {
      lines.push(`...and ${positions.size - 20} more`);
      break;
    }
    const costSol = (pos.costBasisLamports / 1e9).toFixed(6);
    const remaining = pos.remainingAmount;
    const ageSec = Math.floor((Date.now() - pos.entryTime) / 1000);
    lines.push(`${mint} — ${remaining} tokens — cost ${costSol} SOL — age ${ageSec}s`);
  }
  return `<b>Positions (${positions.size})</b>\n` + lines.join("\n");
}

async function getTelegramStatsText(): Promise<string> {
  try {
    let journal: any[] = [];
    if (telegramJournalCache && Date.now() - telegramJournalCache.ts < TELEGRAM_JOURNAL_CACHE_TTL_MS) {
      journal = telegramJournalCache.journal;
    } else {
      const raw = await redisGet("trade_journal");
      journal = raw ? JSON.parse(raw) : [];
      telegramJournalCache = { ts: Date.now(), journal };
    }
    const buys = journal.filter((e: any) => e.action === "BUY").length;
    const sells = journal.filter((e: any) => e.action === "SELL").length;
    const pnlPercentSum = journal.reduce((acc: number, e: any) => acc + (e.pnlPercent ?? 0), 0);
    return (
      `<b>Stats</b>\nOpen positions: ${positions.size}\nJournal entries: ${journal.length}\n` +
      `Buys: ${buys} Sells: ${sells}\nApprox PnL% sum: ${pnlPercentSum.toFixed(2)}\n` +
      `Dry-run: ${DRY_RUN ? "yes" : "no"}`
    );
  } catch (e: any) {
    return `<b>Stats</b>\nOpen positions: ${positions.size}\nJournal: unavailable (${e.message})`;
  }
}

async function getTelegramHistoryText(): Promise<string> {
  try {
    let journal: any[] = [];
    if (telegramJournalCache && Date.now() - telegramJournalCache.ts < TELEGRAM_JOURNAL_CACHE_TTL_MS) {
      journal = telegramJournalCache.journal;
    } else {
      const raw = await redisGet("trade_journal");
      journal = raw ? JSON.parse(raw) : [];
      telegramJournalCache = { ts: Date.now(), journal };
    }
    if (journal.length === 0) return "No trade history.";
    const entries = journal.slice(-10).map((e: any) => {
      const ts = new Date(e.timestamp).toISOString();
      return `${ts} ${e.action} ${e.mint} ${(e.solAmount / 1e9).toFixed(4)} SOL ${e.signature ?? ""}`;
    });
    return `<b>History (last ${entries.length})</b>\n` + entries.join("\n");
  } catch (e: any) {
    return `History unavailable: ${e.message}`;
  }
}

async function getTelegramSettingsText(): Promise<string> {
  return (
    `<b>Settings</b>\nBUY_AMOUNT_SOL: ${(BUY_AMOUNT / 1e9)}\nSLIPPAGE_BPS: ${SLIPPAGE_BPS}\n` +
    `DRY_RUN: ${DRY_RUN}\nPUMPPORTAL: ${PUMPPORTAL_ENABLED}\nONLY_DIRECT_ROUTES: ${ONLY_DIRECT_ROUTES}`
  );
}

/* ================= CRASH ALERTS ================= */

process.on("uncaughtException", async (err) => {
  console.log("❌ UNCAUGHT EXCEPTION:", err.message);
  await sendTelegramAlert(`❌ <b>BOT CRASHED</b>\n${err.message}\nRender should auto-restart it.`);
});

process.on("unhandledRejection", async (reason: any) => {
  console.log("❌ UNHANDLED REJECTION:", reason);
  await sendTelegramAlert(`❌ <b>BOT ERROR</b>\n${String(reason).slice(0, 200)}`);
});

/* ================= PUMP.FUN FAST-PATH DECODER ================= */

let pumpEventCoder: BorshEventCoder | null = null;

// Anchor's IDL decoder rejects newer Pump.fun TradeEvent versions when fields
// are appended. These leading fields have remained stable across versions.
const PUMP_TRADE_EVENT_DISCRIMINATOR = Buffer.from([189, 219, 127, 211, 78, 230, 97, 238]);
const PUMP_TRADE_EVENT_MIN_BYTES = 89; // discriminator + mint + amounts + isBuy + user

async function initPumpDecoder() {
  try {
    const provider = new AnchorProvider(connection, {} as any, {});
    const idl = await Program.fetchIdl(PUMPFUN_PROGRAM, provider);
    if (!idl) {
      console.log("âš ï¸ Could not fetch Pump.fun IDL â€” fast path disabled.");
      return;
    }
    pumpEventCoder = new BorshEventCoder(idl as any);
    console.log("âœ… Pump.fun fast-path decoder ready");
  } catch (e: any) {
    console.log("âš ï¸ Pump.fun decoder init failed:", e.message);
  }
}

type PumpTradeEvent = { mint: string; isBuy: boolean; solAmount: number; user: string };

function tryFastDecode(logs: string[]): PumpTradeEvent | null {
  for (const log of logs) {
    if (!log.startsWith("Program data: ")) continue;
    try {
      const encoded = log.slice("Program data: ".length);
      const raw = Buffer.from(encoded, "base64");

      // Pump.fun has added fields to TradeEvent. Decode the stable prefix
      // directly so new event versions still stay on the no-RPC fast path.
      if (
        raw.length >= PUMP_TRADE_EVENT_MIN_BYTES &&
        raw.subarray(0, 8).equals(PUMP_TRADE_EVENT_DISCRIMINATOR)
      ) {
        return {
          mint: new PublicKey(raw.subarray(8, 40)).toBase58(),
          solAmount: Number(raw.readBigUInt64LE(40)),
          isBuy: raw[56] === 1,
          user: new PublicKey(raw.subarray(57, 89)).toBase58(),
        };
      }

      if (!pumpEventCoder) continue;
      const decoded = pumpEventCoder.decode(encoded);
      if (decoded?.name === "TradeEvent") {
        const d: any = decoded.data;
        const mint = d.mint?.toString();
        const user = d.user?.toString();
        if (!mint || !user) continue;
        return {
          mint,
          isBuy: Boolean(d.isBuy),
          solAmount: Number(d.solAmount ?? 0),
          user,
        };
      }
    } catch {
      // not a TradeEvent line, ignore
    }
  }
  return null;
}

/* ================= DYNAMIC COMPUTE UNITS ================= */

async function simulateAndGetComputeUnits(
  connection: Connection,
  instructions: any[],
  payer: PublicKey
): Promise<number> {
  try {
    const message = new TransactionMessage({
      payerKey: payer,
      recentBlockhash: (await connection.getLatestBlockhash("processed")).blockhash,
      instructions: [
        ComputeBudgetProgram.setComputeUnitLimit({ units: 1_400_000 }), // max for simulation
        ...instructions,
      ],
    }).compileToV0Message();

    const tx = new VersionedTransaction(message);
    tx.sign([wallet]);

    const simResult = await connection.simulateTransaction(tx, {
      replaceRecentBlockhash: true,
      sigVerify: false,
    });

    if (simResult.value.err) {
      throw new Error(`Simulation failed: ${JSON.stringify(simResult.value.err)}`);
    }

    const unitsConsumed = simResult.value.unitsConsumed ?? 0;
    if (unitsConsumed > 0) {
      // Add 15% buffer, cap at 1.4M
      return Math.min(Math.ceil(unitsConsumed * 1.15), 1_400_000);
    }
  } catch (e: any) {
    console.log("⚠️ Simulation failed, using default CU:", e.message);
  }
  return LOCAL_PUMP_COMPUTE_UNITS; // fallback to fixed 200k
}

/* ================= JITO TIP FLOOR ================= */

function getJitoTipLamports(buyAmountLamports: number): number {
  const calculatedTip = Math.floor(buyAmountLamports * (JITO_TIP_BPS / 10000));
  const minTip = 1_000_000; // 0.001 SOL minimum
  return Math.max(calculatedTip, minTip);
}

function JITO_TIP_LAMPORTS(buyAmountLamports: number = BUY_AMOUNT): number {
  return getJitoTipLamports(buyAmountLamports);
}

function logFeeSizingWarning() {
  const currentPriorityFee = getDynamicPriorityFee();
  const totalFeeLamports = JITO_TIP_LAMPORTS() + currentPriorityFee;
  const pctOfBuy = (totalFeeLamports / BUY_AMOUNT) * 100;
  console.log(
    `💸 Fee sizing: Jito tip + priority fee ≈ ${(totalFeeLamports / 1e9).toFixed(4)} SOL ` +
    `(~${pctOfBuy.toFixed(1)}% of ${(BUY_AMOUNT / 1e9).toFixed(4)} SOL buy)`
  );
  console.log(
    `🎯 Effective slippage: ${(SLIPPAGE_BPS / 100).toFixed(0)}% — ` +
    `if this is not 30, a SLIPPAGE_BPS env var on Render is overriding the code`
  );
}

/* ================= NETWORK ================= */

async function fetchJson(url: string, options: any = {}, retries = 2): Promise<any> {
  const baseDelay = 300;
  for (let i = 0; i <= retries; i++) {
    try {
      const res: any = await fetch(url, { agent, ...options });
      if (res.status === 429) {
        if (i === retries) throw new Error(`HTTP 429 Too Many Requests: ${url}`);
        const delay = Math.min(10_000, baseDelay * Math.pow(2, i)) + Math.floor(Math.random() * 100);
        await new Promise((r) => setTimeout(r, delay));
        continue;
      }
      if (!res.ok) {
        const txt = await res.text().catch(() => "");
        throw new Error(`HTTP ${res.status}: ${txt}`);
      }
      return await res.json();
    } catch (e: any) {
      if (i === retries) throw e;
      const delay = Math.min(10_000, baseDelay * Math.pow(2, i)) + Math.floor(Math.random() * 100);
      await new Promise((r) => setTimeout(r, delay));
    }
  }
}

function getPriorityFee() {
  // Use dynamic priority fee for Jupiter swaps
  return getDynamicPriorityFee();
}

function jupHeaders(extra: Record<string, string> = {}) {
  return JUP_API_KEY ? { "x-api-key": JUP_API_KEY, ...extra } : extra;
}

/* ================= BLOCKHASH CACHE ================= */

let cachedBlockhash: string | null = null;
let cachedBlockhashAt = 0;

// Background blockhash refresher - keeps cache warm without blocking hot path
let blockhashRefreshInterval: NodeJS.Timeout | null = null;

function startBlockhashRefresher() {
  if (blockhashRefreshInterval) return;
  // Refresh immediately on start
  refreshBlockhash();
  // Then every 10 seconds in background
  blockhashRefreshInterval = setInterval(refreshBlockhash, 10_000);
  console.log("âœ… Blockhash background refresher started (10s interval)");
}

function stopBlockhashRefresher() {
  if (blockhashRefreshInterval) {
    clearInterval(blockhashRefreshInterval);
    blockhashRefreshInterval = null;
  }
}

function getFastestSubmissionConnection(): Connection {
  return submissionConnections[0] ?? buyExecConnection;
}

async function refreshBlockhash() {
  try {
    // Use the fastest submission RPC for lowest blockhash latency.
    const fastest = getFastestSubmissionConnection();
    const { blockhash } = await fastest.getLatestBlockhash("processed");
    cachedBlockhash = blockhash;
    cachedBlockhashAt = Date.now();
  } catch {
    // keep the old one
  }
}

async function getBlockhashFast(): Promise<string> {
  // Very short TTL since we refresh in background
  if (cachedBlockhash && Date.now() - cachedBlockhashAt < 15_000) return cachedBlockhash;
  // Fallback: synchronous refresh (should rarely hit this)
  await refreshBlockhash();
  if (cachedBlockhash && Date.now() - cachedBlockhashAt < 15_000) return cachedBlockhash;
  const fastest = getFastestSubmissionConnection();
  return (await fastest.getLatestBlockhash("processed")).blockhash;
}

/* ================= WALLET BALANCE CHECK ================= */

let lastLowBalanceAlertAt = 0;
const LOW_BALANCE_ALERT_COOLDOWN_MS = 30 * 60 * 1000;

let lastKnownBalanceLamports: number | null = null;

async function checkWalletBalance() {
  try {
    const lamports = await connection.getBalance(wallet.publicKey, "confirmed");
    lastKnownBalanceLamports = lamports;
    const sol = lamports / 1e9;
    if (sol < MIN_WALLET_BALANCE_SOL) {
      const now = Date.now();
      if (now - lastLowBalanceAlertAt > LOW_BALANCE_ALERT_COOLDOWN_MS) {
        lastLowBalanceAlertAt = now;
        console.log(`\u{26A0} Wallet balance low: ${sol.toFixed(4)} SOL`);
        await sendTelegramAlert(
          `\u{26A0} <b>LOW WALLET BALANCE</b>\nCurrent: ${sol.toFixed(4)} SOL\nThreshold: ${MIN_WALLET_BALANCE_SOL} SOL`
        );
      }
    }
  } catch (e: any) {
    console.log("Balance check failed:", e.message);
  }
}

/* ================= PRICE / MARKET CAP ================= */

async function fetchPriceAndMarketCap(mint: string): Promise<{
  tokenPriceUSD: number | null;
  marketCapUSD: number | null;
}> {
  try {
    const priceRes = await fetchJson(`https://api.jup.ag/price/v3?ids=${mint}`, {
      headers: jupHeaders(),
    });
    const tokenPriceUSD = priceRes?.[mint]?.usdPrice ?? null;

    const supplyInfo = await connection.getTokenSupply(new PublicKey(mint));
    const supply = supplyInfo?.value?.uiAmount ?? null;

    const marketCapUSD = tokenPriceUSD && supply ? tokenPriceUSD * supply : null;
    return { tokenPriceUSD, marketCapUSD };
  } catch (e: any) {
    console.log("Price/MC fetch failed:", e.message);
    return { tokenPriceUSD: null, marketCapUSD: null };
  }
}

function fmtMC(mc: number | null): string {
  if (mc === null) return "unknown";
  return mc >= 1000 ? `${(mc / 1000).toFixed(1)}K` : `${mc.toFixed(0)}`;
}

// On a seconds-old mint the supply/price lookups ALWAYS fail ("could not
// find account") and just burn rate-limit budget at the worst moment.
// Delay them off the hot buy window â€” the alert only needs them later.
function delayedMarketCapSnapshot(mint: string, delayMs = 2500) {
  return new Promise<{ tokenPriceUSD: number | null; marketCapUSD: number | null }>((resolve) => {
    setTimeout(() => resolve(fetchPriceAndMarketCap(mint)), delayMs);
  });
}

/* ================= PERSISTENCE (Upstash Redis) ================= */

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

async function redisSet(key: string, value: string) {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) return;
  try {
    await fetch(`${UPSTASH_URL}/set/${key}`, {
      agent,
      method: "POST",
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
      body: value,
    });
  } catch (e: any) {
    console.log(`Redis set(${key}) error:`, e.message);
  }
}

async function redisGet(key: string): Promise<string | null> {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) return null;
  try {
    const res = await fetch(`${UPSTASH_URL}/get/${key}`, {
      agent,
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
    });
    const data: any = await res.json();
    return data?.result ?? null;
  } catch (e: any) {
    console.log(`Redis get(${key}) error:`, e.message);
    return null;
  }
}

// Upstash leader-lock helpers for Telegram polling
async function acquireTelegramLeaderLock(key: string, ttlSec: number): Promise<boolean> {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) return true; // best-effort: allow polling when Upstash not configured
  try {
    const url = `${UPSTASH_URL}/set/${encodeURIComponent(key)}?nx=true&ex=${Math.max(1, Math.floor(ttlSec))}`;
    const res = await fetch(url, {
      agent,
      method: "POST",
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}`, "Content-Type": "text/plain" },
      body: "1",
    });
    // Upstash returns a JSON result object for set; treat any truthy `result` as success
    const data: any = await res.json().catch(() => null);
    return !!data?.result || res.status === 200;
  } catch (e: any) {
    console.log(`acquireTelegramLeaderLock(${key}) error:`, e?.message ?? e);
    return false;
  }
}

async function releaseTelegramLeaderLock(key: string): Promise<void> {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) return;
  try {
    await fetch(`${UPSTASH_URL}/del/${encodeURIComponent(key)}`, {
      agent,
      method: "POST",
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
    });
  } catch (e: any) {
    console.log(`releaseTelegramLeaderLock(${key}) error:`, e?.message ?? e);
  }
}

async function persistPositions() {
  const serialized = JSON.stringify(Array.from(positions.entries()));
  await redisSet("positions", serialized);
}

async function loadPositions() {
  const raw = await redisGet("positions");
  if (raw) {
    const entries = JSON.parse(raw);
    for (const [mint, pos] of entries) positions.set(mint, pos);
    console.log(`âœ… Restored ${entries.length} position(s) from before restart`);
  }
}

/* ================= TRADE JOURNAL ================= */

interface JournalEntry {
  mint: string;
  action: "BUY" | "SELL";
  reason?: string;
  solAmount: number;
  entryMarketCapUSD: number | null;
  exitMarketCapUSD: number | null;
  pnlPercent: number | null;
  timestamp: number;
  signature: string;
}

const MAX_JOURNAL_ENTRIES = 500;

async function appendTradeJournal(entry: JournalEntry) {
  try {
    const raw = await redisGet("trade_journal");
    const journal: JournalEntry[] = raw ? JSON.parse(raw) : [];
    journal.push(entry);
    while (journal.length > MAX_JOURNAL_ENTRIES) journal.shift();
    await redisSet("trade_journal", JSON.stringify(journal));
  } catch (e: any) {
    console.log("Journal append error:", e.message);
  }
}

/* ================= STATE ================= */

interface Position {
  mint: string;
  originalAmount: number;
  remainingAmount: number;
  costBasisLamports: number;
  entryTime: number;
  highestValue: number;
  entryMarketCapUSD: number | null;
}

const positions = new Map<string, Position>();
const inFlight = new Set<string>();
const seenSignatures = new Set<string>();

function alreadySeen(sig: string): boolean {
  if (seenSignatures.has(sig)) return true;
  seenSignatures.add(sig);
  if (seenSignatures.size > 5000) {
    const iter = seenSignatures.values();
    for (let i = 0; i < 1000; i++) {
      const v = iter.next().value;
      if (v === undefined) break;
      seenSignatures.delete(v);
    }
  }
  return false;
}

let pumpPortalConnected = false;

/* ================= EMERGENCY STOP VIA TELEGRAM ================= */

let botPaused = false;
let telegramUpdateOffset = 0;
const processedUpdateIds = new Set<number>();
let telegramPollingDisabledUntil = 0;
const TELEGRAM_POLL_INTERVAL_MS = Number(process.env.TELEGRAM_POLL_INTERVAL_MS ?? 30_000);
const TELEGRAM_GETUPDATES_TIMEOUT_SEC = Number(process.env.TELEGRAM_GETUPDATES_TIMEOUT_SEC ?? 30);
const TELEGRAM_POLL_BACKOFF_MS = Number(process.env.TELEGRAM_POLL_BACKOFF_MS ?? 5 * 60 * 1000);

async function pollTelegramCommands() {
  if (TELEGRAM_WEBHOOK_MODE) return;
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;
  if (Date.now() < telegramPollingDisabledUntil) return;
  try {
    // Use long polling (timeout in seconds) to reduce frequency of requests.
    const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/getUpdates?offset=${telegramUpdateOffset}&timeout=${TELEGRAM_GETUPDATES_TIMEOUT_SEC}`;
    let data: any;
    try {
      data = await fetchJson(url, {}, 2);
    } catch (e: any) {
      const msg = String(e?.message ?? e);
      // Detect Telegram 409 conflict (another getUpdates owner) and back off to avoid constant 409 spam.
      if (msg.includes("409") || /Conflict|terminated by other getUpdates/i.test(msg)) {
        console.log(`[33mTelegram polling conflict detected: ${msg}. Backing off ${TELEGRAM_POLL_BACKOFF_MS / 1000}s[0m`);
        telegramPollingDisabledUntil = Date.now() + TELEGRAM_POLL_BACKOFF_MS;
        return;
      }
      throw e;
    }
    if (!data?.result) return;
    if (!data?.result) return;

    for (const update of data.result) {
      const updateId = update.update_id;
      if (updateId == null || processedUpdateIds.has(updateId)) continue;
      processedUpdateIds.add(updateId);
      if (processedUpdateIds.size > 1000) {
        // Keep set from growing unbounded
        const first = processedUpdateIds.values().next().value;
        if (first !== undefined) processedUpdateIds.delete(first);
      }

      telegramUpdateOffset = updateId + 1;
      const msg = update.message;
      if (!msg || String(msg.chat?.id) !== String(TELEGRAM_CHAT_ID)) continue;
      await handleTelegramCommandMessage(msg);
    }
  } catch (e: any) {
    console.log("Telegram poll error:", e.message);
  }
}

/* ================= JUPITER (fallback path) ================= */

function normalizeJupiterQuoteResponse(data: any): any | null {
  if (!data) return null;
  if (Array.isArray(data)) return data[0] ?? null;
  if (data?.data && Array.isArray(data.data)) return data.data[0] ?? null;
  if (data?.data) return data.data;
  if (data?.quote) return data.quote;
  return data;
}

function normalizeJupiterSwapResponse(data: any): string | null {
  if (!data) return null;
  if (typeof data.swapTransaction === "string") return data.swapTransaction;
  if (data?.data?.swapTransaction && typeof data.data.swapTransaction === "string") return data.data.swapTransaction;
  if (typeof data.transaction === "string") return data.transaction;
  return null;
}

async function getQuote(inputMint: string, outputMint: string, amount: number, attempts = 4) {
  if (!isJupiterAvailable()) return null;
  if (skipJupiterForMint(outputMint)) return null;

  let lastError: string | null = null;
  for (let i = 0; i < attempts; i++) {
    const useDirect = ONLY_DIRECT_ROUTES && i < 2;
    const params = new URLSearchParams({
      inputMint,
      outputMint,
      amount: Math.floor(amount).toString(),
      slippageBps: SLIPPAGE_BPS.toString(),
      onlyDirectRoutes: useDirect ? "true" : "false",
    });

    for (const baseUrl of [JUP_BASE, JUP_LEGACY_BASE]) {
      const url = `${baseUrl}/quote?${params.toString()}`;
      try {
        const data = await fetchJson(url, { headers: jupHeaders() });
        const quote = normalizeJupiterQuoteResponse(data);
        if (quote && !quote.error && quote.outAmount) {
          if (baseUrl !== JUP_BASE) console.log(`ℹ️ Jupiter quote served by fallback host: ${baseUrl}`);
          return quote;
        }

        const normalizedError = data?.error ?? quote?.error ?? "empty quote response";
        console.log(`🚫 Jupiter quote invalid response (${baseUrl}):`, JSON.stringify(data).slice(0, 1200));
        lastError = String(normalizedError);

        if (String(normalizedError).toLowerCase().includes("no routes")) {
          jupiterNoRouteCache.set(outputMint, Date.now() + JUP_FALLBACK_TTL_MS);
          disableJupiter(`no routes found for ${outputMint}`, 10_000);
          return null;
        }
      } catch (e: any) {
        console.log(`🚫 Jupiter quote fetch error (${baseUrl}):`, String(e?.message ?? e));
        lastError = String(e?.message ?? e);
        if (String(lastError).includes("ENOTFOUND") || String(lastError).includes("ECONNREFUSED")) {
          disableJupiter(`Jupiter host unreachable (${baseUrl})`, 10_000);
        }
      }
    }

    if (i < attempts - 1) await new Promise((r) => setTimeout(r, 600));
  }

  if (lastError) {
    console.log("🚫 Quote failed after retries:", String(lastError).slice(0, 140));
  }
  return null;
}

async function buildSwapTx(quoteResponse: any) {
  const quote = normalizeJupiterQuoteResponse(quoteResponse);
  if (!quote) return null;

  for (const baseUrl of [JUP_BASE, JUP_LEGACY_BASE]) {
    try {
      const data = await fetchJson(`${baseUrl}/swap`, {
        method: "POST",
        headers: jupHeaders({ "Content-Type": "application/json" }),
        body: JSON.stringify({
          quoteResponse: quote,
          userPublicKey: wallet.publicKey.toString(),
          wrapAndUnwrapSol: true,
          prioritizationFeeLamports: getPriorityFee(),
          dynamicComputeUnitLimit: false,
        }),
      });
      const swapTx = normalizeJupiterSwapResponse(data);
      if (swapTx) {
        if (baseUrl !== JUP_BASE) console.log(`ℹ️ Jupiter swap served by fallback host: ${baseUrl}`);
        return swapTx;
      }
      console.log(`🚫 Jupiter swap invalid response (${baseUrl}):`, JSON.stringify(data).slice(0, 1200));
      if (data?.error && String(data.error).toLowerCase().includes("no routes")) {
        disableJupiter(`Jupiter swap no routes`, 10_000);
        return null;
      }
    } catch (e: any) {
      console.log(`🚫 Jupiter swap fetch error (${baseUrl}):`, String(e?.message ?? e));
      if (String(e?.message ?? "").includes("ENOTFOUND") || String(e?.message ?? "").includes("ECONNREFUSED")) {
        disableJupiter(`Jupiter host unreachable (${baseUrl})`, 10_000);
      }
    }
  }

  return null;
}

/* ================= DUAL-PATH SUBMISSION ================= */

// Multi-RPC endpoints for parallel submission (first success wins)
const SUBMISSION_RPCS = [...new Set([
  process.env.BUY_EXEC_RPC_URL,
  process.env.RPC_URL,
  "https://solana-rpc.publicnode.com",
  "https://api.mainnet-beta.solana.com",
].filter((v): v is string => Boolean(v)))];

// submissionConnections will be probed & ranked on startup
let submissionConnections: Connection[] = [];
let submissionPairs: { url: string; conn: Connection }[] = [];
const submissionRpcPenalties = new Map<string, number>(); // higher = worse

async function probeSubmissionConnections(): Promise<void> {
  const probes = await Promise.allSettled(
    SUBMISSION_RPCS.map(async (url) => {
      const conn = new Connection(url, { commitment: "processed", httpAgent: agent });
      const start = Date.now();
      try {
        await conn.getVersion();
        const rt = Date.now() - start;
        return { url, conn, ok: true, rt };
      } catch (e: any) {
        const errMsg = e?.message ?? String(e);
        // Return the error message so we can log it for debugging
        return { url, conn, ok: false, rt: Number.POSITIVE_INFINITY, err: errMsg } as any;
      }
    })
  );

  const results: { url: string; conn: Connection; ok: boolean; rt: number; err?: string }[] = probes
    .filter((p: any) => p.status === "fulfilled")
    .map((p: any) => p.value)
    .sort((a, b) => a.rt - b.rt);

  submissionPairs = results.map(r => ({ url: r.url, conn: r.conn }));
  submissionConnections = submissionPairs.map(p => p.conn);
  console.log(
    `âœ… Submission RPCs ranked: ${results
      .map(r => `${r.url}(${r.ok ? r.rt + 'ms' : 'err' + (r.err ? (': ' + r.err.slice(0,120)) : '')})`)
      .join(', ')}`
  );
}

// Warm submission RPCs periodically to avoid cold TLS/RPC stalls
function startSubmissionWarmup() {
  setInterval(async () => {
    for (const conn of submissionConnections) {
      try {
        await conn.getVersion();
      } catch {
        // ignore
      }
    }
    // Decay penalties slowly so endpoints can recover
    for (const [url, score] of submissionRpcPenalties.entries()) {
      const next = Math.max(0, score - 1);
      if (next === 0) submissionRpcPenalties.delete(url);
      else submissionRpcPenalties.set(url, next);
    }
  }, 30_000);
}

// Priority fee sampling RPCs (only use ones that support getRecentPrioritizationFees)
const PRIORITY_FEE_RPCS = [
  process.env.BUY_EXEC_RPC_URL,
  process.env.RPC_URL,
].filter((v): v is string => Boolean(v));

const priorityFeeConnections = PRIORITY_FEE_RPCS.map(url =>
  new Connection(url, { commitment: "processed", httpAgent: agent })
);

// Dynamic priority fee cache (refreshed every 15s)
let dynamicPriorityFeeLamports = BASE_PRIORITY_FEE;
let lastPriorityFeeUpdate = 0;
const PRIORITY_FEE_TTL_MS = 15_000;

async function refreshPriorityFee() {
  try {
    if (FORCE_PRIORITY_FEE > 0) {
      dynamicPriorityFeeLamports = FORCE_PRIORITY_FEE;
      lastPriorityFeeUpdate = Date.now();
      console.log(`⚡ Using forced priority fee: ${FORCE_PRIORITY_FEE} lamports`);
      return;
    }
    // Fetch recent prioritization fees from Helius RPCs only (others return 0)
    const fees = await Promise.allSettled(
      priorityFeeConnections.map(c => c.getRecentPrioritizationFees())
    );
    const allFees: number[] = [];
    for (const f of fees) {
      if (f.status === "fulfilled" && f.value) {
        console.log(`[PriorityFee] RPC returned ${f.value.length} fee samples`);
        allFees.push(...f.value.map(x => x.prioritizationFee));
      } else if (f.status === "rejected") {
        console.log(`[PriorityFee] RPC error:`, f.reason?.message);
      }
    }
    if (allFees.length === 0) {
      console.log("⚠️ No priority fee data from Helius, using base fee");
      return;
    }
    allFees.sort((a, b) => a - b);
    const p95 = allFees[Math.floor(allFees.length * 0.95)] ?? BASE_PRIORITY_FEE;
    // If P95 is 0 (Helius free tier limitation), use base fee
    if (p95 === 0) {
      console.log("⚠️ P95 priority fee is 0 (RPC limitation), using base fee");
      return;
    }
    // Add 20% buffer, cap at 5x base
    dynamicPriorityFeeLamports = Math.min(Math.ceil(p95 * 1.2), BASE_PRIORITY_FEE * 5);
    lastPriorityFeeUpdate = Date.now();
    console.log(`⚡ Dynamic priority fee updated: ${dynamicPriorityFeeLamports} lamports (P95: ${p95}, samples: ${allFees.length})`);
  } catch (e: any) {
    console.log("⚠️ Priority fee refresh failed:", e.message);
  }
}

// Refresh priority fee periodically (skip if a forced priority fee is configured)
if (FORCE_PRIORITY_FEE === 0) {
  setInterval(refreshPriorityFee, PRIORITY_FEE_TTL_MS);
  // Initial fetch
  refreshPriorityFee();
} else {
  // Ensure forced fee is recorded in the cache
  dynamicPriorityFeeLamports = FORCE_PRIORITY_FEE;
  lastPriorityFeeUpdate = Date.now();
  console.log(`⚡ Using forced priority fee (no RPC sampling): ${FORCE_PRIORITY_FEE} lamports`);
}

function getDynamicPriorityFee(): number {
  if (FORCE_PRIORITY_FEE > 0) return FORCE_PRIORITY_FEE;
  // Fall back to base if cache is stale
  if (Date.now() - lastPriorityFeeUpdate > PRIORITY_FEE_TTL_MS * 2) {
    return BASE_PRIORITY_FEE;
  }
  return dynamicPriorityFeeLamports;
}

function scheduleRebroadcasts(rawBytes: Uint8Array) {
  let sends = 0;
  const timer = setInterval(async () => {
    sends++;
    if (sends > 4) {
      clearInterval(timer);
      return;
    }
    try {
      await broadcastConnection.sendRawTransaction(rawBytes, {
        skipPreflight: true,
        maxRetries: 0,
      });
    } catch {
      // ignore
    }
  }, 500);
}

async function submitSignedTransaction(rawBytes: Uint8Array, useJito = true): Promise<string | null> {
  const signed = VersionedTransaction.deserialize(rawBytes);
  const signature = bs58.encode(signed.signatures[0]!);

  // Parallel multi-RPC submit: fire to all endpoints simultaneously, first success wins
  // Order RPC attempts by adaptive penalty (lower penalty first)
  const orderedPairs = [...submissionPairs].sort((a, b) => (submissionRpcPenalties.get(a.url) ?? 0) - (submissionRpcPenalties.get(b.url) ?? 0));
  const rpcAttempts = orderedPairs.map(({ url, conn }) =>
    conn.sendRawTransaction(rawBytes, { skipPreflight: true, maxRetries: 0 })
      .then(() => "RPC")
      .catch((err: any) => {
        submissionRpcPenalties.set(url, (submissionRpcPenalties.get(url) ?? 0) + 1);
        console.log(`⚠️ RPC submit failure ${url}:`, err?.message ?? String(err));
        return null;
      })
  );

  // Always attempt Jito bundle submission first. This offers the best chance for
  // same-block or next-block inclusion compared to plain RPC.
  let jitoAttempt: Promise<string | null> = Promise.resolve(null);
  if (useJito && JITO_BLOCK_ENGINE_URL) {
    jitoAttempt = (async () => {
      try {
        const blockhash = await getBlockhashFast();
        const tipLamports = JITO_TIP_LAMPORTS();
        const res = await fetchJson(JITO_BLOCK_ENGINE_URL, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            transactions: [bs58.encode(rawBytes)],
            aggregation: "none",
            maxNumberOfTransactionsInBundle: 1,
            bundleSigner: wallet.publicKey.toString(),
            tip: tipLamports,
            clientId: "solana-sniper-bot",
            blockhash,
          }),
        });
        if (res?.result) {
          console.log(`✅ Jito accepted bundle: ${signature}`);
          return "Jito";
        }
        throw new Error(`Jito rejected bundle: ${JSON.stringify(res)}`);
      } catch (e: any) {
        console.log("⚠️ Jito submit error:", e?.message ?? String(e));
        return null;
      }
    })();
  }

  // bloXroute submission (free tier) - 3rd path
  let bloxrouteAttempt: Promise<string | null> = Promise.resolve(null);
  if (BLOXROUTE_ENABLED) {
    bloxrouteAttempt = (async () => {
      try {
        const res = await fetch(BLOXROUTE_ENDPOINT, {
          agent,
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Authorization": BLOXROUTE_AUTH!,
          },
          body: JSON.stringify({
            transaction: bs58.encode(rawBytes),
            skip_preflight: true,
          }),
        });
        const data = await res.json();
        if (data?.result?.signature) {
          console.log(`âœ… bloXroute accepted tx: ${signature}`);
          return "bloXroute";
        }
        throw new Error(`bloXroute submit failed: ${JSON.stringify(data)}`);
      } catch (e: any) {
        console.log("⚠️ bloXroute submit error:", e?.message ?? String(e));
        return null;
      }
    })();
  }

  scheduleRebroadcasts(rawBytes);
  try {
    // Race Jito/bloXroute and all RPC endpoints simultaneously. If Jito accepts,
    // it will usually win, but we still keep RPC as a hot fallback.
    const allAttempts = [jitoAttempt, ...rpcAttempts];
    if (BLOXROUTE_ENABLED) allAttempts.push(bloxrouteAttempt);

    const via = await Promise.any(
      allAttempts.map(async (attempt) => {
        const result = await attempt;
        if (!result) throw new Error("no successful submission path");
        return result;
      })
    );

    console.log(`💡 Transaction submitted via ${via}:`, signature);
    return signature;
  } catch {
    return null;
  }
}

async function sendSwapDual(txBase64: string): Promise<string | null> {
  const swapTx = VersionedTransaction.deserialize(Buffer.from(txBase64, "base64"));
  swapTx.sign([wallet]);
  return submitSignedTransaction(swapTx.serialize());
}

async function sendRawTransactionDual(rawTx: Uint8Array): Promise<string | null> {
  return submitSignedTransaction(rawTx);
}

async function validateBuyTransaction(rawTx: Uint8Array): Promise<boolean> {
  const tx = VersionedTransaction.deserialize(rawTx);

  async function simulate(conn: Connection): Promise<boolean> {
    const result = await conn.simulateTransaction(tx, {
      commitment: "processed",
      replaceRecentBlockhash: true,
      sigVerify: false,
    });
    if (result.value.err) {
      const logs = result.value.logs?.filter(Boolean).join(" | ") ?? "no simulation logs";
      console.log("Buy candidate simulation failed:", JSON.stringify(result.value.err), logs);
      return false;
    }
    return true;
  }

  try {
    return await simulate(buyExecConnection);
  } catch (error: any) {
    console.log("⚠️ Buy candidate simulation unavailable on buyExecConnection:", error.message);
    try {
      return await simulate(connection);
    } catch (fallbackError: any) {
      console.log("⚠️ Buy candidate simulation unavailable on fallback RPC:", fallbackError.message);
      // If simulation cannot be completed due to RPC availability, allow the candidate
      // to proceed so the bot can still attempt the buy on-chain.
      return true;
    }
  }
}

 /* ================= PUMPPORTAL-BUILT TRANSACTIONS ================= */
 // Build direct Pump.fun buys locally from the official SDK. This avoids the
 // PumpPortal HTTP hop and uses the current bonding-curve accounts for the mint.
 async function buildLocalPumpBuyTx(mint: string, slippageBps = SLIPPAGE_BPS): Promise<Uint8Array | null> {
  const startMs = nowMs();
  try {
    const mintKey = new PublicKey(mint);
    const latestBlockhash = await getBlockhashFast();
    const [mintInfo, global] = await Promise.all([
      pumpSdkConnection.getAccountInfo(mintKey, "processed"),
      pumpSdkForBuild.fetchGlobal(), // dedicated SDK RPC
    ]);
    if (!mintInfo) throw new Error("mint account not found");

    const tokenProgram = mintInfo.owner;
    if (!tokenProgram.equals(TOKEN_PROGRAM_ID) && !tokenProgram.equals(TOKEN_2022_PROGRAM_ID)) {
      throw new Error(`unsupported token program: ${tokenProgram.toBase58()}`);
    }

    const cachedWarmState = buyStateWarmCache.get(mint);
    const [buyState, mintState, feeConfig] = await Promise.all([
      cachedWarmState?.timestamp && Date.now() - cachedWarmState.timestamp < 30_000
        ? Promise.resolve(cachedWarmState.buyState)
        : pumpSdkForBuild.fetchBuyState(mintKey, wallet.publicKey, tokenProgram),
      getMint(pumpSdkConnection, mintKey, "processed", tokenProgram),
      cachedWarmState?.timestamp && Date.now() - cachedWarmState.timestamp < 30_000
        ? Promise.resolve(cachedWarmState.feeConfig)
        : pumpSdkForBuild.fetchFeeConfig(),
    ]);

    if (!cachedWarmState || !cachedWarmState.global) {
      buyStateWarmCache.set(mint, { timestamp: Date.now(), buyState, mintState, feeConfig, global });
    }

    const solAmount = new BN(BUY_AMOUNT);
    const bondingCurve = buyState.bondingCurve as any;
    if (!bondingCurve) throw new Error("missing bonding curve state");
    if (bondingCurve.complete || new BN(bondingCurve.realTokenReserves).isZero()) {
      throw new Error("bonding curve is complete; PumpSwap route required");
    }
    if (bondingCurve.quoteMint && !new PublicKey(bondingCurve.quoteMint).equals(new PublicKey(SOL_MINT))) {
      throw new Error(`unsupported quote mint: ${bondingCurve.quoteMint.toString()}`);
    }

    const tokenAmount = getBuyTokenAmountFromSolAmount({
      global,
      feeConfig,
      mintSupply: typeof mintState.supply === "bigint" ? new BN(mintState.supply.toString()) : mintState.supply,
      bondingCurve,
      amount: solAmount,
      quoteMint: new PublicKey(SOL_MINT),
    });
    if (tokenAmount.isZero()) throw new Error("bonding curve returned zero tokens");

    const instructions = await PUMP_SDK.buyInstructions({
      global,
      ...buyState,
      mint: mintKey,
      user: wallet.publicKey,
      amount: tokenAmount,
      solAmount,
      slippage: slippageBps / 100,
      tokenProgram,
    });
    if (!Array.isArray(instructions) || instructions.length === 0) {
      throw new Error("buyInstructions returned no instructions");
    }

    const dynamicFee = getDynamicPriorityFee();
    const dynamicComputeUnits = LOCAL_PUMP_COMPUTE_UNITS; // avoid hot-path simulation
    const priorityMicroLamports = Math.ceil((dynamicFee * 1_000_000) / dynamicComputeUnits);
    const message = new TransactionMessage({
      payerKey: wallet.publicKey,
      recentBlockhash: latestBlockhash,
      instructions: [
        ComputeBudgetProgram.setComputeUnitLimit({ units: dynamicComputeUnits }),
        ComputeBudgetProgram.setComputeUnitPrice({ microLamports: priorityMicroLamports }),
        ...instructions,
      ],
    }).compileToV0Message();
    const tx = new VersionedTransaction(message);
    tx.sign([wallet]);

    const elapsed = nowMs() - startMs;
    console.log(`✅ Local Pump.fun build succeeded (${elapsed}ms): ${mint}`);
    return tx.serialize();
  } catch (e: any) {
    const elapsed = nowMs() - startMs;
    console.log(`❌ Local Pump.fun build unavailable for ${mint}:`, e?.message ?? String(e), `(${elapsed}ms)`);
    return null;
  }
}

// Attempt to reconstruct the pre-buy market-cap using the current cache,
// but prime the mint-derived state first if the cache is empty so a valid buy
// is not rejected before the local Pump SDK path has a chance to fetch it.
async function reconstructPreTradeSnapshotFromCache(mint: string, solAmountLamports: number) {
  try {
    const primeStart = nowMs();
    const primed = await primeMintDerivedState(mint, primeStart).catch((e: any) => {
      console.log(`PREBUY_MC: PRIME_FALLBACK_FAILED mint=${mint} dur=${nowMs() - primeStart}ms err=${e?.message ?? String(e)}`);
      return false;
    });
    if (!primed) {
      console.log(`PREBUY_MC: cache-prime attempted for ${mint} (primed=${primed})`);
    }

    const cached = bondingCurveCache.get(mint);
    const warm = buyStateWarmCache.get(mint);
    const missingTop: string[] = [];
    if (!cached) missingTop.push("bondingCurveCache");
    if (!warm) missingTop.push("buyStateWarmCache");
    if (missingTop.length > 0) {
      const diagTop = {
        bondingCurveCache: !!cached,
        buyStateWarmCache: !!warm,
      };
      console.log(`PREBUY_MC: UNKNOWN reason=missing_top ${missingTop.join(",")} diag=${JSON.stringify(diagTop)}`);
      return null;
    }

    // Narrow types for TS after presence checks
    const cachedState = cached!;
    const warmState = warm!;

    const Q_post = cachedState.virtualQuoteReserves; // BN
    const T_post = cachedState.virtualTokenReserves; // BN
    const realTokenReserves = cachedState.realTokenReserves;
    const mintSupplyRaw = cachedState.tokenTotalSupply ?? warmState.mintState?.supply;
    const feeConfig = warmState.feeConfig;
    const missing: string[] = [];
    if (!Q_post) missing.push("virtualQuoteReserves");
    if (!T_post) missing.push("virtualTokenReserves");
    if (!realTokenReserves) missing.push("realTokenReserves");
    if (!mintSupplyRaw) missing.push("tokenTotalSupply");
    if (!feeConfig) missing.push("feeConfig");
    if (missing.length > 0) {
      const diag = {
        bondingCurveCache_present: !!cached,
        buyStateWarmCache_present: !!warm,
        virtualTokenReserves: T_post ? (typeof T_post.toString === 'function' ? T_post.toString() : String(T_post)) : null,
        virtualQuoteReserves: Q_post ? (typeof Q_post.toString === 'function' ? Q_post.toString() : String(Q_post)) : null,
        realTokenReserves: realTokenReserves ? (typeof realTokenReserves.toString === 'function' ? realTokenReserves.toString() : String(realTokenReserves)) : null,
        realQuoteReserves: cachedState.realQuoteReserves ? (typeof cachedState.realQuoteReserves.toString === 'function' ? cachedState.realQuoteReserves.toString() : String(cachedState.realQuoteReserves)) : null,
        tokenTotalSupply_cached: cachedState.tokenTotalSupply != null ? (typeof cachedState.tokenTotalSupply.toString === 'function' ? cachedState.tokenTotalSupply.toString() : String(cachedState.tokenTotalSupply)) : null,
        mintState_supply: warmState.mintState?.supply ?? null,
        feeConfig_present: !!feeConfig,
      };
      console.log(`PREBUY_MC: UNKNOWN reason=missing ${missing.join(",")} diag=${JSON.stringify(diag)}`);
      return null;
    }

    // Extract fee tiers from feeConfig. Prefer SDK shape: feeConfig.feeTiers
    // where each tier has { marketCapLamportsThreshold: BN, fees: { protocolFeeBps, creatorFeeBps } }
    let sdkFeeTiers: Array<{ threshold: BN | null; totalFeeBps: number }> = [];
    try {
      if (feeConfig) {
        if (Array.isArray(feeConfig.feeTiers) && feeConfig.feeTiers.length > 0) {
          sdkFeeTiers = feeConfig.feeTiers.map((t: any) => {
            const thr = t.marketCapLamportsThreshold ?? t.marketCapLamportsThreshold ?? t.marketCap ?? null;
            const thrBN = thr == null ? null : (typeof thr === "object" && typeof thr.toString === "function" ? new BN(thr.toString()) : new BN(String(thr)));
            const protocol = t.fees?.protocolFeeBps ?? t.protocolFeeBps ?? 0;
            const creator = t.fees?.creatorFeeBps ?? t.creatorFeeBps ?? 0;
            const p = typeof protocol === "object" && typeof protocol.toString === "function" ? Number(new BN(protocol.toString()).toString()) : Number(protocol || 0);
            const c = typeof creator === "object" && typeof creator.toString === "function" ? Number(new BN(creator.toString()).toString()) : Number(creator || 0);
            return { threshold: thrBN, totalFeeBps: p + c };
          });
        } else if (Array.isArray(feeConfig.tiers) && feeConfig.tiers.length > 0) {
          // fallback shape
          sdkFeeTiers = feeConfig.tiers.map((t: any) => ({ threshold: null, totalFeeBps: Number(t.feeBps) }));
        } else if (Array.isArray(feeConfig) && feeConfig.length > 0) {
          sdkFeeTiers = feeConfig.map((t: any) => ({ threshold: null, totalFeeBps: Number(t.feeBps) }));
        }
      }
    } catch (error) {
      const errorMessage = error instanceof Error ? error.message : String(error);
      console.log(`PREBUY_MC: UNKNOWN reason=feeTiers_parse_error ${errorMessage}`);
      return null;
    }

    if (sdkFeeTiers.length === 0) {
      const feeDiag = {
        feeConfig_present: !!feeConfig,
        feeConfig_keys: feeConfig ? Object.keys(feeConfig).slice(0, 10) : null,
        feeConfig_sample: feeConfig && typeof feeConfig === 'object' ? JSON.stringify(Object.entries(feeConfig).slice(0, 5)) : null,
        sdkFeeTiers_length: sdkFeeTiers.length,
      };
      console.log(`PREBUY_MC: UNKNOWN reason=missing_feeTiers diag=${JSON.stringify(feeDiag)}`);
      return null;
    }

    const candidateBps = [...new Set(sdkFeeTiers.map((t) => t.totalFeeBps))];
    const solAmtBn = new BN(solAmountLamports ?? 0);
    if (solAmtBn.lte(new BN(0))) return null;

    const LAMPORTS = new BN(1e9);
    const solPriceUsd = await getSolPriceUsd() ?? Number(warmState.global?.solPriceUsd ?? warmState.global?.solUsdPrice ?? 0);
    if (!solPriceUsd || solPriceUsd <= 0) {
      const solDiag = {
        env_SOL_PRICE_USD: process.env.SOL_PRICE_USD ?? null,
        warm_global_present: !!warmState.global,
        warm_global_keys: warmState.global ? Object.keys(warmState.global).slice(0, 10) : null,
        solPriceUsd_resolved: solPriceUsd,
      };
      console.log(`PREBUY_MC: UNKNOWN reason=missing_solPriceUsd diag=${JSON.stringify(solDiag)}`);
      return null;
    }

    let lastSelectedFeeBps: number | null = null;
    let lastTotalFeeBps: number | null = null;
    for (const totalFeeBps of candidateBps) {
      lastTotalFeeBps = totalFeeBps;
      // inputAmount = floor((amount - 1) * 10000 / (10000 + totalFeeBps))
      const A = solAmtBn.subn(1).mul(new BN(10000));
      const D = new BN(10000 + totalFeeBps);
      if (A.lt(new BN(0))) continue;
      const inputAmount = A.div(D);
      if (Q_post.lte(inputAmount)) continue;
      const numer = inputAmount.mul(T_post);
      const denom = Q_post.sub(inputAmount);
      if (denom.lte(new BN(0))) continue;
      const xCand = numer.div(denom);
      let finalTokens = xCand;
      if (realTokenReserves && xCand.gte(realTokenReserves)) finalTokens = realTokenReserves;

      const virtualTokenReserves_pre = T_post.add(finalTokens);
      const virtualQuoteReserves_pre = Q_post.sub(inputAmount);
      if (virtualTokenReserves_pre.lte(new BN(0)) || virtualQuoteReserves_pre.lte(new BN(0))) continue;

      const mintSupplyBN = typeof mintSupplyRaw === "bigint" ? new BN(mintSupplyRaw.toString()) : new BN(String(mintSupplyRaw));

      const preMarketCapQuote = virtualQuoteReserves_pre.mul(mintSupplyBN).div(virtualTokenReserves_pre); // in lamports*units
      // Convert quote (lamports) to SOL and then to USD
      const preMarketCapSol = Number(preMarketCapQuote.div(LAMPORTS).toString());
      const preMarketCapUsd = preMarketCapSol * solPriceUsd;

      // Match the exact Pump SDK fee-tier semantics: threshold is in lamports, and
      // the tier is selected by marketCap >= threshold, with the first tier used for
      // marketCap below the lowest threshold. There is no "totalFeeBps" comparison in
      // the SDK; the selected tier is the one whose `fees` object is returned.
      let selectedFeeBps: number | null = null;
      try {
        const tiersWithThresh = sdkFeeTiers.filter((t): t is { threshold: BN; totalFeeBps: number } => t.threshold != null);
        if (tiersWithThresh.length > 0) {
          const sorted = tiersWithThresh.slice().sort((a, b) => a.threshold.cmp(b.threshold));
          const firstTier = sorted[0];
          const preMarketCapLamports = preMarketCapQuote;
          if (!firstTier) {
            selectedFeeBps = null;
          } else if (preMarketCapLamports.lt(firstTier.threshold)) {
            selectedFeeBps = firstTier.totalFeeBps;
          } else {
            const matchedTier = sorted.slice().reverse().find((tier) => preMarketCapLamports.gte(tier.threshold));
            selectedFeeBps = matchedTier?.totalFeeBps ?? firstTier.totalFeeBps;
          }
        } else {
          const firstSdk = sdkFeeTiers[0];
          selectedFeeBps = firstSdk ? firstSdk.totalFeeBps : null;
        }
      } catch {
        selectedFeeBps = null;
      }

      lastSelectedFeeBps = selectedFeeBps;
      if (selectedFeeBps != null && Number(selectedFeeBps) === Number(totalFeeBps)) {
        return { tokenPriceUSD: null, marketCapUSD: preMarketCapUsd };
      }
    }

    const feeDiag = {
      selectedFeeBps: lastSelectedFeeBps,
      totalFeeBps: lastTotalFeeBps,
      candidateBps,
      sdkFeeTiers: sdkFeeTiers.map((tier) => ({
        threshold: tier.threshold ? tier.threshold.toString() : null,
        totalFeeBps: tier.totalFeeBps,
      })),
      feeConfig: feeConfig ? {
        feeTiers: Array.isArray(feeConfig.feeTiers) ? feeConfig.feeTiers.map((tier: any) => ({
          marketCapLamportsThreshold: tier.marketCapLamportsThreshold ? tier.marketCapLamportsThreshold.toString() : null,
          fees: tier.fees ? {
            lpFeeBps: tier.fees.lpFeeBps ? tier.fees.lpFeeBps.toString() : null,
            protocolFeeBps: tier.fees.protocolFeeBps ? tier.fees.protocolFeeBps.toString() : null,
            creatorFeeBps: tier.fees.creatorFeeBps ? tier.fees.creatorFeeBps.toString() : null,
          } : null,
        })) : null,
        flatFees: feeConfig.flatFees ? {
          lpFeeBps: feeConfig.flatFees.lpFeeBps ? feeConfig.flatFees.lpFeeBps.toString() : null,
          protocolFeeBps: feeConfig.flatFees.protocolFeeBps ? feeConfig.flatFees.protocolFeeBps.toString() : null,
          creatorFeeBps: feeConfig.flatFees.creatorFeeBps ? feeConfig.flatFees.creatorFeeBps.toString() : null,
        } : null,
      } : null,
    };
    console.log(`PREBUY_MC: UNKNOWN reason=no_fee_tier_match selectedFeeBps=${lastSelectedFeeBps ?? "null"} totalFeeBps=${lastTotalFeeBps ?? "null"} candidateBps=${JSON.stringify(candidateBps)} sdkFeeTiers=${JSON.stringify(feeDiag.sdkFeeTiers)} feeConfig=${JSON.stringify(feeDiag.feeConfig)}`);
    return null;
  } catch (error) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    console.log(`PREBUY_MC: UNKNOWN reason=exception ${errorMessage}`);
    return null;
  }
}

async function buildLocalPumpSwapBuyTx(mint: string, slippageBps = SLIPPAGE_BPS): Promise<Uint8Array | null> {
  const startMs = nowMs();
  try {
    const mintKey = new PublicKey(mint);
    const latestBlockhash = await getBlockhashFast();
    const canonicalPoolKey = canonicalPumpPoolPda(mintKey, new PublicKey(SOL_MINT));
    let poolKey = canonicalPoolKey;
    let poolOrigin = "canonical";
    const canonicalPool = await pumpSdkConnection.getAccountInfo(canonicalPoolKey, "processed");

    if (!canonicalPool) {
      const poolAccounts = await pumpSdkConnection.getProgramAccounts(PUMP_AMM_PROGRAM_ID, {
        commitment: "processed",
        filters: [{ memcmp: { offset: 43, bytes: mintKey.toBase58() } }],
      });
      for (const account of poolAccounts) {
        try {
          const pool = PUMP_AMM_SDK.decodePool(account.account);
          if (pool.baseMint.equals(mintKey) && pool.quoteMint.equals(new PublicKey(SOL_MINT))) {
            poolKey = account.pubkey;
            poolOrigin = "noncanonical";
            break;
          }
        } catch {
          // Ignore other Pump AMM accounts that happen to match the filter.
        }
      }
    }

    if (poolKey.equals(canonicalPoolKey) && !canonicalPool) {
      throw new Error("PumpSwap pool not visible yet");
    }
    if (poolOrigin === "noncanonical") {
      console.log(`ℹ️ Using noncanonical PumpSwap pool for ${mint}: ${poolKey.toBase58()}`);
    }

    const swapState = await pumpAmmSdkForBuild.swapSolanaState(poolKey, wallet.publicKey);
    if (!swapState || !swapState.pool) throw new Error("missing PumpSwap swap state");
    if (!swapState.pool.quoteMint.equals(new PublicKey(SOL_MINT))) {
      throw new Error(`unsupported PumpSwap quote mint: ${swapState.pool.quoteMint.toBase58()}`);
    }

    const instructions = await PUMP_AMM_SDK.buyQuoteInput(swapState, new BN(BUY_AMOUNT), slippageBps / 100);
    if (!Array.isArray(instructions) || instructions.length === 0) {
      throw new Error("buyQuoteInput returned no instructions");
    }

    const dynamicFee = getDynamicPriorityFee();
    const dynamicComputeUnits = await simulateAndGetComputeUnits(pumpSdkConnection, instructions, wallet.publicKey);
    const priorityMicroLamports = Math.ceil((dynamicFee * 1_000_000) / dynamicComputeUnits);
    const message = new TransactionMessage({
      payerKey: wallet.publicKey,
      recentBlockhash: latestBlockhash,
      instructions: [
        ComputeBudgetProgram.setComputeUnitLimit({ units: dynamicComputeUnits }),
        ComputeBudgetProgram.setComputeUnitPrice({ microLamports: priorityMicroLamports }),
        ...instructions,
      ],
    }).compileToV0Message();
    const tx = new VersionedTransaction(message);
    tx.sign([wallet]);

    const elapsed = nowMs() - startMs;
    console.log(`✅ PumpSwap build succeeded (${elapsed}ms) via ${poolOrigin} pool: ${mint}`);
    return tx.serialize();
  } catch (error: any) {
    const elapsed = nowMs() - startMs;
    console.log(`❌ PumpSwap local build unavailable for ${mint}:`, error?.message ?? String(error), `(${elapsed}ms)`);
    return null;
  }
}

// PumpPortal builds the current, correct Pump.fun transaction server-side â€”
// they maintain the account layouts through every Pump.fun breaking change,
// which is exactly the burden this bot should not carry itself. The response
// is an UNSIGNED transaction: the private key never leaves this bot. We sign
// locally and submit through our own dual path (Jito + RPC + rebroadcasts).
// One HTTP round trip, works on seconds-old tokens (no indexing lag).
// Cost: 0.5% of trade size. No API key required for this endpoint.

async function buildPumpPortalTx(
  action: "buy" | "sell",
  mint: string,
  amount: number | string,
  denominatedInSol: boolean,
  slippageBps = SLIPPAGE_BPS,
  pool: "pump" | "auto" = "auto"
): Promise<Uint8Array | null> {
  const url = "https://pumpportal.fun/api/trade-local";
  for (let attempt = 1; attempt <= 1; attempt++) {
    try {
      const res: any = await fetch(url, {
        agent: pumpPortalAgent,
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          publicKey: wallet.publicKey.toString(),
          action,
          mint,
          amount,
          denominatedInSol,
          slippage: slippageBps / 100,
          priorityFee: getDynamicPriorityFee() / 1e9,
          pool,
        }),
      });

      if (!res.ok) {
        const errText = await res.text().catch(() => "");
        const truncated = String(errText).slice(0, 240);
        console.log(`✖ trade-local ${action} build failed (pool=${pool}, attempt=${attempt}): status=${res.status} body=${truncated}`);
        if ((res.status >= 500 || res.status === 429 || res.status === 408) && attempt < 2) {
          continue;
        }
        return null;
      }

      const buf = new Uint8Array(await res.arrayBuffer());
      const tx = VersionedTransaction.deserialize(buf);
      tx.sign([wallet]);

      // NOTE: removed pre-submit simulation for hot-path speed. Deterministic
      // failures (such as Anchor 6005) will be handled post-submit or via
      // background reconciliation. This keeps the builder fast and non-blocking.

      return tx.serialize();
    } catch (e: any) {
      const message = e?.message ?? String(e);
      console.log(`✖ trade-local ${action} error (pool=${pool}, attempt=${attempt}):`, message);
      if (attempt < 2) {
        continue;
      }
      return null;
    }
  }
  return null;
}
/* ================= BALANCE ================= */

let tokenBalanceRpcFlip = 0;

async function getTokenBalance(mint: string): Promise<number> {
  try {
    const tokenMint = new PublicKey(mint);
    const rpcs = [connection, broadcastConnection];
    const conn = rpcs[tokenBalanceRpcFlip++ % rpcs.length]!;
    const res = await conn.getParsedTokenAccountsByOwner(wallet.publicKey, { mint: tokenMint });
    return res.value.reduce(
      (sum: number, account: any) => sum + Number(account.account?.data?.parsed?.info?.tokenAmount?.amount ?? 0),
      0
    );
  } catch {
    return 0;
  }
}

type SignatureCheck = "confirmed" | "failed" | "pending";

async function waitForConfirmation(signature: string, attempts = 20, delayMs = 500): Promise<SignatureCheck> {
  // One RPC per attempt (alternating). Hammering both providers at once
  // was a major source of the 429 storms that degraded the hot path.
  const rpcs = [connection, broadcastConnection];
  for (let i = 0; i < attempts; i++) {
    try {
      const conn = rpcs[i % rpcs.length]!;
      const check = await conn.getSignatureStatuses([signature], { searchTransactionHistory: true });
      const status = check.value[0];
      if (status) {
        if (status.err) {
          console.log("âŒ Buy transaction failed on-chain:", signature, JSON.stringify(status.err));
          await logFailedTransaction(signature);
          return "failed";
        }
        if (status.confirmationStatus === "confirmed" || status.confirmationStatus === "finalized") return "confirmed";
      }
    } catch {}
    if (i < attempts - 1) await new Promise((r) => setTimeout(r, delayMs));
  }
  return "pending";
}

async function logFailedTransaction(signature: string): Promise<void> {
  try {
    const tx = await connection.getTransaction(signature, {
      commitment: "confirmed",
      maxSupportedTransactionVersion: 0,
    });
    const logs = tx?.meta?.logMessages?.filter(Boolean) ?? [];
    if (logs.length > 0) console.log("On-chain program logs:", logs.join(" | "));
  } catch (error: any) {
    console.log("Could not fetch failed transaction logs:", error.message);
  }
}

async function waitForBalance(mint: string, attempts = 20, delayMs = 500): Promise<number> {
  for (let i = 0; i < attempts; i++) {
    const bal = await getTokenBalance(mint);
    if (bal > 0) return bal;
    await new Promise((r) => setTimeout(r, delayMs));
  }
  return 0;
}

/* ================= FILTER ================= */

function passesFilters(quote: any) {
  const impact = Number(quote.priceImpactPct ?? 0);
  if (impact > MAX_PRICE_IMPACT) {
    console.log(
      `âŒ Quote rejected â€” price impact ${(impact * 100).toFixed(1)}% above ${(MAX_PRICE_IMPACT * 100).toFixed(0)}% cap`
    );
    return false;
  }
  return Number(quote.outAmount) > 0;
}

/* ================= BUY ================= */
// Direct Pump.fun is now tried FIRST â€” no Jupiter round trip for tokens still
// on the bonding curve, which is every real trade from this target wallet.
// Jupiter only runs as a fallback if the curve read fails or the token has
// already migrated off the bonding curve.

// Build Jupiter buy transaction (returns signed Uint8Array for parallel race)
async function buildJupiterBuyTx(mint: string) {
  const quote = await getQuote(SOL_MINT, mint, BUY_AMOUNT);
  if (!quote) {
    console.log(`🚫 Jupiter quote unavailable for ${mint}`);
    return null;
  }
  if (!passesFilters(quote)) {
    console.log(`🚫 Jupiter quote rejected by filters for ${mint}`);
    return null;
  }
  const txBase64 = await buildSwapTx(quote);
  if (!txBase64) {
    console.log(`🚫 Jupiter swap transaction unavailable for ${mint}`);
    return null;
  }
  const swapTx = VersionedTransaction.deserialize(Buffer.from(txBase64, "base64"));
  try {
    const blockhash = await getBlockhashFast();
    if (swapTx.message && typeof (swapTx.message as any).recentBlockhash === "string") {
      (swapTx.message as any).recentBlockhash = blockhash;
    }
  } catch (e: any) {
    console.log(`⚠️ Jupiter build blockhash refresh failed for ${mint}:`, e.message ?? String(e));
  }
  swapTx.sign([wallet]);
  return swapTx.serialize();
}

async function submitJupiterBuy(mint: string) {
  const quote = await getQuote(SOL_MINT, mint, BUY_AMOUNT);
  if (!quote || !passesFilters(quote)) return null;
  const tx = await buildSwapTx(quote);
  if (!tx) return null;
  const sig = await sendSwapDual(tx);
  if (sig) console.log("ðŸš€ BUY submitted (Jupiter):", mint, sig);
  return sig;
}

async function executeBuy(
    mint: string,
    targetSnapshotPromise?: Promise<{ tokenPriceUSD: number | null; marketCapUSD: number | null }>,
    solAmountLamports?: number,
    detectionMs?: number,
    skipMcCheck: boolean = false
  ) {
    // Atomic duplicate guard — must be first, before any async
    if (positions.has(mint) || inFlight.has(mint)) {
      console.log("⏸ Buy skipped — already holding or in-flight:", mint);
      return;
    }
    inFlight.add(mint);

    if (botPaused) {
      console.log("⏸ Buy skipped — bot is paused:", mint);
      inFlight.delete(mint);
      return;
    }

    // Circuit breaker check
    if (isCircuitBreakerActive()) {
      const remaining = getCircuitBreakerRemainingMs();
      console.log(`🚫 Buy skipped — circuit breaker active (${(remaining / 1000).toFixed(0)}s remaining):`, mint);
      inFlight.delete(mint);
      return;
    }

    const requiredLamports = BUY_AMOUNT + JITO_TIP_LAMPORTS() + 3_000_000;
    if (lastKnownBalanceLamports !== null && lastKnownBalanceLamports < requiredLamports) {
      console.log(
        `\⏸ Buy skipped — wallet ${(lastKnownBalanceLamports / 1e9).toFixed(4)} SOL below required ~${(requiredLamports / 1e9).toFixed(4)} SOL:`,
        mint
      );
      inFlight.delete(mint);
      return;
    }

    const detectionTs = detectionMs ?? nowMs();
    let preTradeSnapshot: { tokenPriceUSD: number | null; marketCapUSD: number | null } | null = null;
    if (targetSnapshotPromise) {
      preTradeSnapshot = await targetSnapshotPromise;
    } else {
      // Attempt to reconstruct a pre-trade snapshot from existing cached data
      try {
        console.log(`⚡ PREBUY_MC_CHECK start: ${nowMs() - detectionTs}ms after detection`);
        const startCheck = nowMs();
        preTradeSnapshot = await reconstructPreTradeSnapshotFromCache(mint, solAmountLamports ?? 0);
        const checkElapsed = nowMs() - startCheck;
        console.log(`⚡ PREBUY_MC_CHECK done: ${checkElapsed}ms`);
      } catch (e: any) {
        preTradeSnapshot = { tokenPriceUSD: null, marketCapUSD: null };
      }
      if (!preTradeSnapshot) preTradeSnapshot = { tokenPriceUSD: null, marketCapUSD: null };
    }
    const preTradeMc = preTradeSnapshot.marketCapUSD ?? null;
    console.log(`⚡ PREBUY_MC: ${preTradeMc === null ? 'UNKNOWN' : '$' + preTradeMc.toFixed(2)} (checked at ${nowMs() - detectionTs}ms)`);
    if (shouldRejectPreTradeMarketCap(preTradeMc)) {
      if (preTradeMc === null || preTradeMc === undefined) {
        console.log(`🚫 PRE-TRADE MC REJECTED: UNKNOWN — bonding curve state unavailable for ${mint}`);
      } else {
        console.log(
          `🚫 PRE-TRADE MC rejected for ${mint}: $${preTradeMc.toFixed(0)} > $${PRE_TRADE_MAX_MARKET_CAP_USD.toFixed(0)}`
        );
      }
      inFlight.delete(mint);
      return;
    }

    // Buy immediately. Market cap is measured after execution for reporting.
    console.log(`⚡ Immediate copy-buy path: ${mint}`);

    const buyStartMs = Date.now();

    try {
      let sig: string | null = null;

      if (DRY_RUN) {
        // Simulate: log what we would do, track latency
        const simulatedSig = `DRYRUN_${Date.now()}_${mint.slice(0, 6)}`;
        console.log(`ðŸ§Œ [DRY-RUN] Would buy: ${mint} | builder race would fire now`);
        sig = simulatedSig;
        dryRunStats.buys++;
        dryRunStats.totalLatencyMs += Date.now() - buyStartMs;
        // Mock PnL estimation (random walk for demo)
        dryRunStats.pnlSol += (Math.random() - 0.45) * 0.01;
      } else {
        const candidates: Array<[string, () => Promise<Uint8Array | null>]> = [];

        if (!isPumpFallbackActive(mint) && PUMPPORTAL_ENABLED) {
          candidates.push(["PumpPortal-trade-local", () => buildPumpPortalTx("buy", mint, BUY_AMOUNT / 1e9, true, SLIPPAGE_BPS, "pump")]);
        } else if (isPumpFallbackActive(mint)) {
          console.log(`ℹ️ Skipping PumpPortal build for ${mint} due to recent Anchor 6005 failures`);
        }

        if (PREFER_PUMPPORTAL_ONLY && PUMPPORTAL_ENABLED && !isPumpFallbackActive(mint)) {
          candidates.length = 0;
          candidates.push(["PumpPortal-trade-local", () => buildPumpPortalTx("buy", mint, BUY_AMOUNT / 1e9, true, SLIPPAGE_BPS, "pump")]);
        }

        candidates.push(["local Pump SDK", () => buildLocalPumpBuyTx(mint, SLIPPAGE_BPS)]);
        const orderedCandidates = orderBuyBuilders(candidates.map(([name, build]) => ({ name, kind: "direct", build }))).map(({ name, build }) => [name, build] as [string, () => Promise<Uint8Array | null>]);

        // V14 hot path: only the direct bonding-curve builders are allowed. No Jupiter, no PumpSwap, no fallback routes.
        const racedCandidates = orderedCandidates.map(async ([builderName, buildFn]) => {
          const bStart = nowMs();
          try {
            console.log(`⚡ BUILD START: ${builderName} for ${mint} (${nowMs() - detectionTs}ms since detection)`);
            const candidate = await buildFn();
            const bElapsed = nowMs() - bStart;
            if (!candidate) {
              console.log(`↪ ${builderName} returned null (${bElapsed}ms)`);
              throw new Error(`${builderName} unavailable`);
            }
            console.log(`✅ BUILDER_READY: ${builderName} (${nowMs() - detectionTs}ms since detection, ${bElapsed}ms build)`);
            return { builderName, rawTx: candidate };
          } catch (e: any) {
            console.log(`⚠️ ${builderName} build error:`, e?.stack ?? e?.message ?? String(e));
            throw new Error(`${builderName} unavailable`);
          }
        });

        let selectedBuilder = "";
        let rawTx: Uint8Array | null = null;
        try {
          const winner = await Promise.any(racedCandidates);
          selectedBuilder = winner.builderName;
          rawTx = winner.rawTx;
        } catch {
          // Every builder failed or produced a transaction rejected by simulation.
        }

        if (!rawTx) {
          console.log("âŒ Buy aborted — all builders failed for", mint);
          return;
        }
        console.log(`⚡ SIGN: signing/submitting tx (${nowMs() - detectionTs}ms since detection)`);
        sig = await sendRawTransactionDual(rawTx);
        if (sig) console.log(`🚀 SUBMITTED (${selectedBuilder}): ${mint} ${sig} (total ${(nowMs() - detectionTs)}ms since detection)`);
        if (!sig) return;
      }

      const myMcPromise = delayedMarketCapSnapshot(mint, 2000);
      const targetMcPromise = Promise.resolve(preTradeSnapshot);
      const confirmationPromise = waitForConfirmation(sig);
      let actual = await waitForBalance(mint);
      let confirmation = await confirmationPromise;

      if (confirmation === "failed") {
        await sendTelegramAlert(
          `❌ <b>BUY FAILED ON-CHAIN</b>\nMint: <code>${mint}</code>\n` +
          `The single submitted transaction failed; no retry was sent.\n` +
          `<a href="https://solscan.io/tx/${sig}">View transaction</a>`
        );
        return;
      }
    if (actual <= 0) {
      const state = confirmation === "confirmed" ? "confirmed transaction, but no token balance" : "transaction still pending and no token balance";
      console.log(`⚠️ Could not confirm ${state} for`, mint, sig);
      await sendTelegramAlert(
        `⚠️ <b>BALANCE WARNING</b>\n${state} for <code>${mint}</code>.\n` +
        `<a href="https://solscan.io/tx/${sig}">View transaction</a>`
      );
      return;
    }

    const [mySnapshot, targetSnapshot] = await Promise.all([myMcPromise, targetMcPromise]);

    positions.set(mint, {
      mint,
      originalAmount: actual,
      remainingAmount: actual,
      costBasisLamports: BUY_AMOUNT,
      entryTime: Date.now(),
      highestValue: 0,
      entryMarketCapUSD: mySnapshot.marketCapUSD,
    });

    await persistPositions();

    const cleanSig = sig;

    await appendTradeJournal({
      mint,
      action: "BUY",
      solAmount: BUY_AMOUNT / 1e9,
      entryMarketCapUSD: mySnapshot.marketCapUSD,
      exitMarketCapUSD: null,
      pnlPercent: null,
      timestamp: Date.now(),
      signature: cleanSig,
    });

    await sendTelegramAlert(
      `\u{1F680} <b>BUY EXECUTED</b>\n` +
      `Mint: <code>${mint}</code>\n` +
      `Amount: ${(BUY_AMOUNT / 1e9).toFixed(3)} SOL\n` +
      `<b>The Man's entry MC:</b> ${fmtMC(targetSnapshot.marketCapUSD)}\n` +
      `<b>Your entry MC:</b> ${fmtMC(mySnapshot.marketCapUSD)}\n` +
      `<a href="https://solscan.io/tx/${cleanSig}">View on Solscan</a>`
    );
  } finally {
    inFlight.delete(mint);
  }
}

/* ================= SELL ================= */

async function executeSell(mint: string, amount: number, reason: string) {
  const pos = positions.get(mint);
  if (!pos) return;

  let sig: string | null = null;

  const quote = await getQuote(mint, SOL_MINT, amount, 3);
  if (quote) {
    const tx = await buildSwapTx(quote);
    if (tx) sig = await sendSwapDual(tx);
  }

  // Fallback for tokens Jupiter cannot quote yet (seconds old): sell via a
  // PumpPortal-built transaction. Pump.fun tokens use 6 decimals.
  if (!sig) {
    console.log("â†ªï¸ Jupiter sell unavailable â€” trying PumpPortal build for", mint);
    const rawTx = await buildPumpPortalTx("sell", mint, amount / 1e6, false);
    if (rawTx) sig = await sendRawTransactionDual(rawTx);
  }

  if (!sig) {
    console.log(`âŒ Sell failed on both paths for ${mint} (${reason})`);
    return;
  }

  console.log(`âœ… SELL (${reason}):`, mint, sig);

  const soldFraction = amount / pos.remainingAmount;
  const costBasisSold = pos.costBasisLamports * soldFraction;
  const proceedsLamports = quote ? Number(quote.outAmount) : 0;
  const pnlPercent = quote ? ((proceedsLamports - costBasisSold) / costBasisSold) * 100 : 0;

  pos.remainingAmount -= amount;
  pos.costBasisLamports -= costBasisSold;

  if (pos.remainingAmount <= 0) positions.delete(mint);
  await persistPositions();

  const { marketCapUSD } = await fetchPriceAndMarketCap(mint);
  const cleanSig = sig;

  await appendTradeJournal({
    mint,
    action: "SELL",
    reason,
    solAmount: proceedsLamports / 1e9,
    entryMarketCapUSD: pos.entryMarketCapUSD,
    exitMarketCapUSD: marketCapUSD,
    pnlPercent,
    timestamp: Date.now(),
    signature: cleanSig,
  });

  await sendTelegramAlert(
    `\u{2705} <b>SELL EXECUTED</b> (${reason})\n` +
    `Mint: <code>${mint}</code>\n` +
    `Market Cap: ${fmtMC(marketCapUSD)}\n` +
    `PnL (this portion): ${pnlPercent >= 0 ? "+" : ""}${pnlPercent.toFixed(1)}%\n` +
    `<a href="https://solscan.io/tx/${cleanSig}">View on Solscan</a>`
  );
}

function targetExitSellAmount(position: Position): number {
  return Math.floor(position.remainingAmount * 0.9);
}

/* ================= DETECTION ================= */

function getResolvedAccountKeys(tx: any): PublicKey[] {
  const staticKeys: PublicKey[] = tx.transaction?.message?.staticAccountKeys ?? [];
  const loaded = tx.meta?.loadedAddresses;
  const writable: PublicKey[] = loaded?.writable ?? [];
  const readonly: PublicKey[] = loaded?.readonly ?? [];
  return [...staticKeys, ...writable, ...readonly];
}

async function getTransactionWithRetry(signature: string, attempts = 8, delayMs = 500) {
  const rpcs = [connection, broadcastConnection];
  for (let i = 0; i < attempts; i++) {
    try {
      const conn = rpcs[i % rpcs.length]!;
      const result = await conn.getTransaction(signature, {
        commitment: "confirmed",
        maxSupportedTransactionVersion: 0,
      });
      if (result?.meta) return result;
    } catch {}
    if (i < attempts - 1) await new Promise((r) => setTimeout(r, delayMs));
  }
  return null;
}

const retryQueue = new Map<string, { attempts: number; timer: NodeJS.Timeout }>();

function queueForRetry(signature: string) {
  if (retryQueue.has(signature)) return;
  const timer = setTimeout(() => processRetry(signature), 3000);
  retryQueue.set(signature, { attempts: 1, timer });
}

async function processRetry(signature: string) {
  const entry = retryQueue.get(signature);
  if (!entry) return;
  retryQueue.delete(signature);

  console.log(`ðŸ”„ Retry #${entry.attempts} for ${signature}`);
  try {
    const tx = await getTransactionWithRetry(signature, 4, 500);
    if (tx?.meta) {
      await handleTxInternal(tx, signature);
      return;
    }
  } catch {}

  if (entry.attempts < 5) {
    const timer = setTimeout(() => processRetry(signature), 3000);
    retryQueue.set(signature, { attempts: entry.attempts + 1, timer });
  } else {
    console.log(`âŒ PERMANENTLY MISSED TRADE: ${signature}`);
    await sendTelegramAlert(
      `ðŸš¨ <b>POSSIBLE MISSED TRADE</b>\nSignature: <code>${signature}</code>\nUnable to fetch after 5 retries. Check Solscan manually.`
    );
  }
}

async function handleTxInternal(tx: any, signature: string): Promise<boolean> {
  try {
    const keys = getResolvedAccountKeys(tx);
    const targetIdx = keys.findIndex((k) => isTargetWalletKey(k));
    if (targetIdx === -1) return false;

    const preTok = tx.meta.preTokenBalances ?? [];
    const postTok = tx.meta.postTokenBalances ?? [];
    for (const pre of preTok) {
      if (!isTargetWalletAddress(pre.owner)) continue;
      if (!positions.has(pre.mint)) continue;
      const post = postTok.find((p: any) => p.owner === pre.owner && p.mint === pre.mint);
      const preAmount = Number(pre.uiTokenAmount.uiAmount ?? 0);
      const postAmount = post ? Number(post.uiTokenAmount.uiAmount ?? 0) : 0;
      if (postAmount < preAmount) {
        const held = positions.get(pre.mint);
        if (held && held.remainingAmount > 0) {
          console.log("ðŸš¨ TARGET EXITED â€” dumping:", pre.mint);
          const sellAmount = targetExitSellAmount(held);
          if (sellAmount > 0) executeSell(pre.mint, sellAmount, "TARGET_EXITED");
          return true;
        }
      }
    }

    const preBalances = tx.meta.preTokenBalances ?? [];
    const postBalances = tx.meta.postTokenBalances ?? [];
    let foundAny = false;
    for (const post of postBalances) {
      if (!isTargetWalletAddress(post.owner)) continue;
      if (post.mint === SOL_MINT) continue;
      const pre = preBalances.find((p: any) => p.owner === post.owner && p.mint === post.mint);
      const preAmount = pre ? Number(pre.uiTokenAmount.uiAmount) : 0;
      const postAmount = Number(post.uiTokenAmount.uiAmount);
      if (postAmount > preAmount) {
        foundAny = true;
        console.log("ðŸ”¥ DETECTED buy:", post.mint);
        // Avoid network MC lookups on the hot path; rely on cached reconstruction.
        const decoded = tryFastDecode(tx.meta.logMessages ?? []);
        const solAmountLamports =
          decoded !== null &&
          decoded.isBuy === true &&
          decoded.mint === post.mint &&
          decoded.user === post.owner &&
          decoded.solAmount > 0
            ? decoded.solAmount
            : 0;
        executeBuy(post.mint, undefined, solAmountLamports, nowMs());
        return true;
      }
    }

    if (!foundAny) {
      console.log("â„¹ï¸ No token increase for target â€” not a buy we can copy.");
    }
    return false;
  } catch (e: any) {
    console.log("handleTxInternal error:", e.message);
    return false;
  }
}

async function handleTx(signature: string) {
  if (seenSignatures.has(signature)) return;
  seenSignatures.add(signature);

  try {
    // The log arrives at `processed`; transaction metadata usually becomes
    // readable shortly afterward. Poll both free RPCs at 250 ms so we catch
    // the first available response instead of waiting a full half-second.
    const tx = await getTransactionWithRetry(signature, 12, 350);
    if (!tx?.meta) {
      console.log("âŒ Could not fetch tx for", signature, "â€” queueing for retry");
      queueForRetry(signature);
      return;
    }
    await handleTxInternal(tx, signature);
  } catch (e: any) {
    console.log("handleTx error:", e.message);
    queueForRetry(signature);
  }
}

/* ================= WEBSOCKET ROTATION ================= */

function onWalletLog(log: Logs) {
  const signature = log.signature;
  if (seenSignatures.has(signature)) return;

  const fast = tryFastDecode(log.logs);
  const isTargetTrade = fast && (() => {
    try { return isTargetWalletAddress(fast.user); } catch { return false; }
  })();

  if (isTargetTrade && fast.isBuy) {
    console.log(`Target wallet activity: https://solscan.io/tx/${signature}`);
    console.log(`⚡ IDL FAST-PATH BUY: ${fast.mint}`);
    seenSignatures.add(signature);
    // Fire and forget - don't await executeBuy to minimize latency
    executeBuy(fast.mint, undefined, fast.solAmount, nowMs()).catch(e =>
      console.log("⚠️ executeBuy error (non-blocking):", e.message)
    );
    return;
  }
  if (isTargetTrade && !fast.isBuy) {
    console.log(`Target wallet activity: https://solscan.io/tx/${signature}`);
    const held = positions.get(fast.mint);
    if (held && held.remainingAmount > 0) {
      console.log("ðŸš¨ Fast-path: target SOLD a token we hold â€” dumping:", fast.mint);
      seenSignatures.add(signature);
      const sellAmount = targetExitSellAmount(held);
      if (sellAmount > 0) executeSell(fast.mint, sellAmount, "TARGET_EXITED").catch(e =>
        console.log("âš ï¸ executeSell error (non-blocking):", e.message)
      );
      return;
    }
  }

  if (fast && !isTargetTrade) return;
  handleTx(signature);
}

interface DetectionChannel {
  conn: Connection;
  subId: number;
}

let activeChannels: DetectionChannel[] = [];
let consecutive429s = 0;
let rotationTimer: ReturnType<typeof setTimeout> | null = null;

function scheduleRotate(delayMs = 60 * 1000) {
  if (rotationTimer) return;
  rotationTimer = setTimeout(() => {
    rotationTimer = null;
    rotateSubscriptions();
  }, delayMs);
}

function rotateSubscriptions() {
  if (PUMPPORTAL_ENABLED) {
    console.log("⚠️ Skipping websocket rotation — PumpPortal is primary detection channel");
    return;
  }
  const urls = uniqueDetectionUrls.length > 0 ? uniqueDetectionUrls : DETECTION_URLS;
  const fresh: DetectionChannel[] = [];
  for (const url of urls) {
    try {
      const conn = makeConnection(url);
      for (const wallet of TARGET_WALLETS) {
        const subId = conn.onLogs(wallet, onWalletLog, "processed");
        fresh.push({ conn, subId });
      }
    } catch (e: any) {
      const msg = e.message ?? String(e);
      console.log("⚠️ Failed to open detection channel for URL:", url, "->", msg);
      if (msg.includes("429")) consecutive429s++;
    }
  }

  if (fresh.length === 0) {
    console.log("⚠️ Rotation produced no channels — keeping the old ones alive");
    // Exponential backoff when encountering repeated 429 rate limits.
    if (consecutive429s > 0) {
      const backoffMs = Math.min(5 * 60 * 1000, 60_000 * Math.pow(2, Math.max(0, consecutive429s - 1)));
      console.log(`🐌 Too many 429s (${consecutive429s}) — backing off next rotation for ${Math.round(backoffMs / 1000)}s`);
      scheduleRotate(backoffMs);
      return;
    }
    scheduleRotate();
    return;
  }

  consecutive429s = 0;

  for (const ch of activeChannels) {
    try {
      // Only unsubscribe if the underlying websocket is OPEN. Calling
      // removeOnLogsListener when the socket is CLOSING/CLOSED can trigger
      // a library-level `logsUnsubscribe` RPC error (readyState 2).
      const ws = (ch.conn as any)._rpcWebSocket;
      if (ws && ws.readyState === 1) {
        try {
          ch.conn.removeOnLogsListener(ch.subId);
        } catch {}
      } else {
        // If socket not open, attempt to close it (best-effort) and skip
        // the unsubscribe RPC which would fail.
        try {
          ws?.close?.();
        } catch {}
      }
    } catch {}
  }

  activeChannels = fresh;
  console.log(`✅ ${fresh.length} detection channel(s) rotated onto fresh websockets`);
  scheduleRotate();
}

/* ================= PUMPPORTAL CHANNEL ================= */

const PUMPPORTAL_API_KEY = process.env.PUMPPORTAL_API_KEY ?? "";
const PUMPPORTAL_WS = PUMPPORTAL_API_KEY
  ? `wss://pumpportal.fun/api/data?api-key=${PUMPPORTAL_API_KEY}`
  : "wss://pumpportal.fun/api/data";

async function resolveWebSocketCtor(): Promise<any> {
  if ((globalThis as any).WebSocket) return (globalThis as any).WebSocket;
  try {
    const mod: any = await import("ws");
    return mod.default ?? mod.WebSocket ?? null;
  } catch {
    return null;
  }
}

async function startPumpPortal() {
  const WS = await resolveWebSocketCtor();
  if (!WS) {
    console.log("\u{26A0} PumpPortal channel DISABLED — no WebSocket support. Set NODE_VERSION=22 on Render, or run: npm i ws");
    await sendTelegramAlert(
      "\u{26A0} <b>PumpPortal channel is OFF</b>\nThis Node version has no WebSocket support. Set NODE_VERSION=22 in Render environment settings (or npm i ws) — this is your fastest detection channel."
    );
    return;
  }

  let reconnectDelayMs = 3000;
  const RECONNECT_DELAY_MAX_MS = 120_000;
  const RECONNECT_DELAY_MIN_MS = 3000;
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null;

  const scheduleReconnect = (delayMs: number) => {
    if (reconnectTimer) return;
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      connect();
    }, delayMs);
  };

  const connect = () => {
    let ws: any;
    try {
      // Validate WS is a callable constructor
      if (typeof WS !== "function") {
        console.log("âš ï¸ PumpPortal: WS is not a function, skipping reconnect");
        scheduleReconnect(30_000);
        return;
      }
      ws = new WS(PUMPPORTAL_WS);
    } catch (e: any) {
      console.log("âš ï¸ PumpPortal WS creation failed:", e.message);
      scheduleReconnect(5000);
      return;
    }

    if (!ws) {
      console.log("âš ï¸ PumpPortal WS returned null/undefined, retrying in 5s");
      setTimeout(connect, 5000);
      return;
    }

    ws.onopen = () => {
      pumpPortalConnected = true;
      reconnectDelayMs = RECONNECT_DELAY_MIN_MS;
      console.log("BOT_STARTUP: PumpPortal connected");
      console.log("âœ… PumpPortal channel connected");
      ws.send(JSON.stringify({ method: "subscribeAccountTrade", keys: TARGET_WALLET_STRINGS }));
      console.log("BOT_STARTUP: subscription sent");
    };

    ws.onmessage = async (ev: any) => {
       try {
         const raw = typeof ev.data === "string" ? ev.data : ev.data.toString();
         const msg = JSON.parse(raw);
         if (!msg?.signature || !msg?.mint || !msg.traderPublicKey) return;

         if (!isTargetWalletAddress(msg.traderPublicKey)) return;
         if (seenSignatures.has(msg.signature)) return;

         if (msg.txType === "sell") {
           const held = positions.get(msg.mint);
           if (held && held.remainingAmount > 0) {
             console.log("ðŸš¨ PumpPortal: target SOLD a token we hold â€” dumping:", msg.mint);
             seenSignatures.add(msg.signature);
             const sellAmount = targetExitSellAmount(held);
             if (sellAmount > 0) executeSell(msg.mint, sellAmount, "TARGET_EXITED");
           }
           return;
         }
         if (msg.txType !== "buy") return;

        console.log("âš¡ PUMPPORTAL detected buy:", msg.mint);
        seenSignatures.add(msg.signature);
        const detectionTs = nowMs();

        await primeMintDerivedState(msg.mint, detectionTs).catch((e: any) => {
          console.log(`PRIME_FAILED: mint=${msg.mint} dur=${nowMs() - detectionTs}ms err=${e?.message ?? String(e)}`);
        });

         // Temporary read-only capture for verifier: save raw PumpPortal event
         // plus any already-cached bonding-curve / warm-state data. This does
         // NOT change buy logic nor perform additional network calls.
         try {
           (async () => {
             try {
               const captureDir = "verifier/real-events";
               await fs.promises.mkdir(captureDir, { recursive: true });
               const cached = bondingCurveCache.get(msg.mint) || null;
               const warm = buyStateWarmCache.get(msg.mint) || null;

               function bnToStr(v: any) {
                 if (v == null) return null;
                 if (typeof v === "object" && typeof v.toString === "function") return v.toString();
                 return String(v);
               }

               const capture: any = {
                 rawEvent: msg,
                 mint: msg.mint,
                 solAmount: msg.solAmount ?? null,
                 signature: msg.signature ?? null,
                 slot: msg.slot ?? null,
                 timestamp: Date.now(),
                 bondingCurveCache: null,
                 buyStateWarmCache: null,
               };

               if (cached) {
                 capture.bondingCurveCache = {
                   associatedBondingCurve: cached.associatedBondingCurve ? String(cached.associatedBondingCurve) : null,
                   virtualQuoteReserves: bnToStr(cached.virtualQuoteReserves),
                   virtualTokenReserves: bnToStr(cached.virtualTokenReserves),
                   realQuoteReserves: bnToStr(cached.realQuoteReserves),
                   realTokenReserves: bnToStr(cached.realTokenReserves),
                   timestamp: cached.timestamp ?? null,
                 };
               }

               if (warm) {
                 capture.buyStateWarmCache = {
                   feeConfig: warm.feeConfig ?? null,
                   mintSupply: warm.mintState?.supply ? String(warm.mintState.supply) : null,
                   buyStateBondingCurve: warm.buyState?.bondingCurve ?? null,
                   timestamp: warm.timestamp ?? null,
                 };
               }

               const fileName = `${Date.now()}-${msg.mint}.json`;
               const filePath = `${captureDir}/${fileName}`;
               await fs.promises.writeFile(filePath, JSON.stringify(capture, null, 2));
               console.log("🔒 Wrote PumpPortal capture:", filePath);
             } catch (e: any) {
               console.log("⚠️ failed to write PumpPortal capture:", e?.message ?? String(e));
             }
           })();
         } catch (e) {
           // swallow errors to avoid impacting hot path
         }

          // Prime bonding curve cache from PumpPortal event (if present)
          // PumpPortal sometimes includes bonding curve account in the trade event
          if (msg.bondingCurve && msg.associatedBondingCurve) {
            try {
              const bondingCurveKey = new PublicKey(msg.bondingCurve);
              const associatedBondingCurveKey = new PublicKey(msg.associatedBondingCurve);
              // Fetch and cache asynchronously, don't block the hot path
              (async () => {
                const primeStart = nowMs();
                console.log(`PRIME_START: mint=${msg.mint} ts=${Date.now()} rel=${primeStart - detectionTs}ms`);
                // Start operations in parallel but capture start times
                const bcStart = nowMs();
                const bcPromise = pumpSdkConnection.getAccountInfo(bondingCurveKey, "processed");
                console.log(`PRIME_OP_START: bondingCurve.getAccountInfo start=${bcStart}`);

                const abcStart = nowMs();
                const abcPromise = pumpSdkConnection.getAccountInfo(associatedBondingCurveKey, "processed");
                console.log(`PRIME_OP_START: associatedBondingCurve.getAccountInfo start=${abcStart}`);

                const feeStart = nowMs();
                const feePromise = pumpSdkForBuild.fetchFeeConfig();
                console.log(`PRIME_OP_START: fetchFeeConfig start=${feeStart}`);

                let bcAccount: any = null;
                let abcAccount: any = null;
                let feeConfig: any = null;
                try {
                  try {
                    bcAccount = await bcPromise;
                    const bcEnd = nowMs();
                    console.log(`PRIME_OP_END: bondingCurve.getAccountInfo end=${bcEnd} dur=${bcEnd - bcStart}ms`);
                  } catch (e: any) {
                    const bcFail = nowMs();
                    console.log(`PRIME_OP_FAILED: bondingCurve.getAccountInfo dur=${bcFail - bcStart}ms err=${e?.message ?? String(e)}`);
                    throw e;
                  }

                  try {
                    abcAccount = await abcPromise;
                    const abcEnd = nowMs();
                    console.log(`PRIME_OP_END: associatedBondingCurve.getAccountInfo end=${abcEnd} dur=${abcEnd - abcStart}ms`);
                  } catch (e: any) {
                    const abcFail = nowMs();
                    console.log(`PRIME_OP_FAILED: associatedBondingCurve.getAccountInfo dur=${abcFail - abcStart}ms err=${e?.message ?? String(e)}`);
                    throw e;
                  }

                  try {
                    feeConfig = await feePromise;
                    const feeEnd = nowMs();
                    console.log(`PRIME_OP_END: fetchFeeConfig end=${feeEnd} dur=${feeEnd - feeStart}ms`);
                  } catch (e: any) {
                    const feeFail = nowMs();
                    console.log(`PRIME_OP_FAILED: fetchFeeConfig dur=${feeFail - feeStart}ms err=${e?.message ?? String(e)}`);
                    throw e;
                  }

                  if (bcAccount && abcAccount && feeConfig) {
                    const data = bcAccount.data;
                    if (data.length >= 8 + 8 + 8 + 8 + 8 + 8) {
                      const virtualTokenReserves = new BN(data.subarray(8, 16).readBigUInt64LE(0).toString());
                      const virtualQuoteReserves = new BN(data.subarray(16, 24).readBigUInt64LE(0).toString());
                      const realTokenReserves = new BN(data.subarray(24, 32).readBigUInt64LE(0).toString());
                      const realQuoteReserves = new BN(data.subarray(32, 40).readBigUInt64LE(0).toString());
                      const tokenTotalSupply = data.length >= 48 ? new BN(data.subarray(40, 48).readBigUInt64LE(0).toString()) : null;
                      bondingCurveCache.set(msg.mint, {
                        bondingCurve: bondingCurveKey,
                        associatedBondingCurve: associatedBondingCurveKey,
                        virtualTokenReserves,
                        virtualQuoteReserves,
                        realTokenReserves,
                        realQuoteReserves,
                        tokenTotalSupply,
                        timestamp: Date.now(),
                      });
                      const primeEnd = nowMs();
                      const warm = buyStateWarmCache.get(msg.mint) || null;
                      const solPriceUsd = Number(process.env.SOL_PRICE_USD ?? warm?.global?.solPriceUsd ?? warm?.global?.solUsdPrice ?? 0);
                      console.log(`PRIME_COMPLETE: mint=${msg.mint} totalDur=${primeEnd - primeStart}ms populated=true buyStateWarmCache=${warm ? "present" : "absent"} fields={virtualTokenReserves:${virtualTokenReserves != null},virtualQuoteReserves:${virtualQuoteReserves != null},realTokenReserves:${realTokenReserves != null},tokenTotalSupply:${tokenTotalSupply != null},feeConfig:${feeConfig != null},solPriceUsd:${!!solPriceUsd}}`);
                    } else {
                      const primeEnd = nowMs();
                      console.log(`PRIME_COMPLETE: mint=${msg.mint} totalDur=${primeEnd - primeStart}ms populated=false reason=insufficient_account_data_len=${data?.length ?? 0}`);
                    }
                  } else {
                    const primeEnd = nowMs();
                    console.log(`PRIME_COMPLETE: mint=${msg.mint} totalDur=${primeEnd - primeStart}ms populated=false reason=missing_accounts_or_feeConfig`);
                  }
                } catch (e: any) {
                  const primeFail = nowMs();
                  console.log(`PRIME_FAILED: mint=${msg.mint} dur=${primeFail - primeStart}ms err=${e?.message ?? String(e)}`);
                }
              })();
            } catch {}
          }

         executeBuy(msg.mint, undefined, msg.solAmount, nowMs()).catch(e =>
            console.log("⚠️ executeBuy error (non-blocking):", e.message)
          );
       } catch {
         // non-JSON frame, ignore
       }
     };

    ws.onclose = (ev: any) => {
      pumpPortalConnected = false;
      const code = ev?.code ?? 0;
      const reason = typeof ev?.reason === "string" ? ev.reason : "";
      console.log(`âš ï¸ PumpPortal channel closed (code=${code}, reason=${reason}) â€” reconnecting in ${reconnectDelayMs / 1000}s`);
      scheduleReconnect(reconnectDelayMs);
      reconnectDelayMs = Math.min(reconnectDelayMs * 2, RECONNECT_DELAY_MAX_MS);
    };

    ws.onerror = (err: any) => {
      const message = err?.message ?? String(err);
      console.log("âš ï¸ PumpPortal WS error:", message);
      if (message.includes("429") || message.includes("Too Many Requests")) {
        reconnectDelayMs = Math.min(reconnectDelayMs * 2, RECONNECT_DELAY_MAX_MS);
      }
      try {
        ws.close();
      } catch {}
    };
  };

  connect();
}

/* ================= POLLING RECONCILIATION LOOP ================= */

const lastPolledSignature = new Map<string, string | null>();

async function initPollingCursor() {
  try {
    for (const wallet of TARGET_WALLETS) {
      const sigs = await connection.getSignaturesForAddress(wallet, { limit: 1 }, "confirmed");
      if (sigs.length > 0 && sigs[0]) {
        lastPolledSignature.set(wallet.toString(), sigs[0].signature);
        console.log("âœ… Polling cursor initialized for", wallet.toString(), "at:", sigs[0].signature);
      } else {
        lastPolledSignature.set(wallet.toString(), null);
      }
    }
  } catch (e: any) {
    console.log("âš ï¸ Could not initialize polling cursor:", e.message);
  }
}

async function pollForMissedTrades() {
  try {
    for (const wallet of TARGET_WALLETS) {
      const options: any = { limit: 25 };
      const lastSig = lastPolledSignature.get(wallet.toString());
      if (lastSig) options.until = lastSig;

      const sigs = await connection.getSignaturesForAddress(wallet, options, "confirmed");
      if (sigs.length === 0) continue;

      const ordered = [...sigs].reverse();

      for (const sigInfo of ordered) {
        if (sigInfo.err) continue;
        if (seenSignatures.has(sigInfo.signature)) continue;
        console.log("ðŸ”Ž POLL found unprocessed signature for", wallet.toString(), ":", sigInfo.signature);
        handleTx(sigInfo.signature);
      }

      lastPolledSignature.set(wallet.toString(), sigs[0]?.signature ?? lastSig ?? null);
    }
  } catch (e: any) {
    console.log("Polling error:", e.message);
  }
}

/* ================= KEEP-ALIVE SERVER ================= */

const app = express();
app.use(express.json());
app.get("/", (_req, res) => res.send("Engine Active"));
app.get("/health", (_req, res) => {
  const mem = process.memoryUsage().heapUsed / 1024 / 1024;
  const wsStatus = pumpPortalConnected ? "connected" : "disconnected";
  res.json({
    status: "ok",
    positions: positions.size,
    paused: botPaused,
    pumpPortal: wsStatus,
    uptime: process.uptime(),
    memoryMB: Math.round(mem),
    timestamp: Date.now(),
  });
});

app.post(TELEGRAM_WEBHOOK_ROUTE, async (req, res) => {
  try {
    const update = req.body ?? {};
    const msg = update.message;
    if (msg) {
      await handleTelegramCommandMessage(msg);
    }
    res.sendStatus(200);
  } catch (err: any) {
    console.error("Webhook error:", err?.message ?? String(err));
    res.sendStatus(500);
  }
});

// Simple HTTP endpoint to pre-warm a specific mint in the prebuild/buyState cache.
// Example: POST /prewarm?mint=So....
app.post('/prewarm', express.json(), async (req, res) => {
  const mint = String(req.query.mint || req.body?.mint || "").trim();
  if (!mint) return res.status(400).json({ error: 'missing mint query/body parameter' });
  try {
    await (async function prewarmMint(mintArg: string) {
      try {
        const m = new PublicKey(mintArg);
        const [buyState, mintState, feeConfig, global] = await Promise.all([
          pumpSdkForBuild.fetchBuyState(m, wallet.publicKey, TOKEN_PROGRAM_ID).catch((e: any) => { throw e; }),
          getMint(pumpSdkConnection, m, 'processed', TOKEN_PROGRAM_ID).catch((e: any) => { throw e; }),
          pumpSdkForBuild.fetchFeeConfig().catch((e: any) => { throw e; }),
          pumpSdkForBuild.fetchGlobal().catch((e: any) => { throw e; }),
        ]);
        buyStateWarmCache.set(mintArg, { timestamp: Date.now(), buyState, mintState, feeConfig, global });
        // Also prime prebuildCache bonding curve data if available
        try {
          const bondingCurveAddress = bondingCurvePda(m);
          const bcAccount = await pumpSdkConnection.getAccountInfo(bondingCurveAddress, 'processed');
          if (bcAccount?.data) {
            prebuildCache.set(mintArg, {
              bondingCurve: bondingCurveAddress,
              associatedBondingCurve: bondingCurveAddress,
              virtualQuoteReserves: new BN(0),
              virtualTokenReserves: new BN(0),
              realQuoteReserves: new BN(0),
              realTokenReserves: new BN(0),
              timestamp: Date.now(),
            });
          }
        } catch {}
      } catch (e: any) {
        throw e;
      }
    })(mint);
    res.json({ ok: true, mint });
  } catch (e: any) {
    res.status(500).json({ error: e?.message ?? String(e) });
  }
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Keep-alive server on port ${PORT}`));

// Optional: keep Render (or any URL) warm by pinging it periodically to avoid cold starts.
const WARM_URL = process.env.WARM_URL ?? process.env.RENDER_WARM_URL ?? "";
if (WARM_URL) {
  const warmInterval = Number(process.env.WARM_PING_INTERVAL_MS ?? 5 * 60 * 1000);
  setInterval(() => {
    fetch(WARM_URL, { agent }).catch(() => {});
  }, warmInterval);
  console.log(`âœ… Keep-alive pings enabled for ${WARM_URL} every ${Math.round(warmInterval / 1000)}s`);
}

/* ================= EARLY PENDING-TRANSACTION FEED ================= */

let pendingSignatureSubscription: number | null = null;
let pendingSignatureDisabled = false;
let pendingSignatureWsErrorUnsubscribe: (() => void) | null = null;

function disablePendingSignatureFeed(reason: string) {
  pendingSignatureDisabled = true;
  if (pendingSignatureSubscription !== null) {
    try { (connection as any).removeSignatureListener(pendingSignatureSubscription); } catch {};
    pendingSignatureSubscription = null;
  }
  if (pendingSignatureWsErrorUnsubscribe) {
    try { pendingSignatureWsErrorUnsubscribe(); } catch {};
    pendingSignatureWsErrorUnsubscribe = null;
  }
  try { (connection as any)._rpcWebSocket?.close?.(); } catch {};
  console.log(`⚠️ Pending-signature feed disabled: ${reason}`);
}

function enablePendingSignatureFeed() {
  if (pendingSignatureDisabled || pendingSignatureSubscription !== null) return;

  const anyConn = connection as any;
  if (typeof anyConn?.onSignature !== "function") {
    console.log("⚠️ Pending-signature feed unavailable on this RPC client");
    return;
  }

  console.log("⚠️ Pending-signature feed is not supported by this bot with the installed web3.js API");
  disablePendingSignatureFeed("pending-signature feed disabled due to unsupported onSignature API");
}

async function probePendingSignatureSupport(): Promise<void> {
  if (pendingSignatureDisabled) {
    console.log("⚠️ Pending-signature probe skipped: pending-signature feed already disabled");
    return;
  }

  const anyConn = connection as any;
  if (typeof anyConn?.onSignature !== "function") {
    console.log("⚠️ Pending-signature probe skipped: onSignature not available");
    return;
  }

  console.log("⚠️ Pending-signature probe disabled: this web3.js version does not support wildcard signature subscriptions via onSignature");
  disablePendingSignatureFeed("pending-signature probe disabled due to unsupported onSignature API");
}

/* ================= START ================= */

// gRPC Detection Manager (Yellowstone/Shyft) - optional
let createGrpcDetectionManager: ((endpoint: string, token: string, targetWallets: string[]) => {
  setTradeHandler: (handler: (event: GrpcTradeEvent) => void) => void;
  setStatusHandler: (handler: (status: string) => void) => void;
  start: () => Promise<void>;
  stop: () => Promise<void>;
  addTargetWallet: (wallet: string) => void;
  removeTargetWallet: (wallet: string) => void;
  getStatus: () => boolean;
}) | null = null;

let grpcManager: {
  setTradeHandler: (handler: (event: GrpcTradeEvent) => void) => void;
  setStatusHandler: (handler: (status: string) => void) => void;
  start: () => Promise<void>;
  stop: () => Promise<void>;
  addTargetWallet: (wallet: string) => void;
  removeTargetWallet: (wallet: string) => void;
  getStatus: () => boolean;
} | null = null;

async function start() {
  console.log("BOT_STARTUP: process entered");
  console.log("ðŸš€ Copy-trade bot starting. Targets:", TARGET_WALLETS.map(w => w.toString()).join(", "));

  logFeeSizingWarning();

  await Promise.all([loadPositions(), initPumpDecoder(), initPollingCursor()]);
  await refreshSolPriceUsd();
  setInterval(() => {
    refreshSolPriceUsd().catch(() => {});
  }, SOL_PRICE_CACHE_TTL_MS);

  // Log Helius WS / detection configuration for debugging 429 sources
  const heliusWsEnv = (process.env.DISABLE_HELIUS_WS ?? "false").toLowerCase() === "true";
  const heliusWsDisabled = heliusWsEnv || PUMPPORTAL_ENABLED;
  console.log(`âœ… Helius WS disabled: ${heliusWsDisabled} (DISABLE_HELIUS_WS=${process.env.DISABLE_HELIUS_WS ?? "unset"}, PUMPPORTAL=${PUMPPORTAL_ENABLED})`);
  console.log(`âœ… Detection channels configured: ${DETECTION_URLS.length} URLs, extra unique: ${uniqueDetectionUrls.length}`);

  // Start background blockhash refresher (replaces old 25s interval)
  startBlockhashRefresher();

  // Enable the earliest available feed we can use from this runtime.
  // The pending-signature listener can trigger on some RPC providers
  // but many hosted endpoints (free Render, some public RPCs) reject
  // `signatureSubscribe`. Enable it explicitly via env var to avoid
  // noisy JSON-RPC errors on unsupported hosts.
  const PENDING_SIGNATURE_ENABLED = (process.env.ENABLE_PENDING_SIGNATURE ?? "auto") !== "false";
  if (PENDING_SIGNATURE_ENABLED) {
    console.log("⚡ Pending-signature feed auto-probing");
    await probePendingSignatureSupport();
  } else {
    console.log("⚠️ Pending-signature feed disabled by env (ENABLE_PENDING_SIGNATURE=false)");
  }

  // Initialize gRPC detection if enabled - dynamically load module
  if (GRPC_ENABLED) {
    try {
      const mod = await import("./grpc-detection.js");
      createGrpcDetectionManager = mod.createGrpcDetectionManager;
      grpcManager = createGrpcDetectionManager(GRPC_ENDPOINT, GRPC_TOKEN, TARGET_WALLETS.map(w => w.toString()));
      grpcManager.setTradeHandler(handleGrpcTradeEvent);
      grpcManager.setStatusHandler((status) => {
        console.log(`[gRPC] Status: ${status}`);
      });
      await grpcManager.start();
      console.log("✅ gRPC detection connected");
    } catch (err: any) {
      if (err.message?.includes("Cannot find native binding") || err.code === "ERR_MODULE_NOT_FOUND") {
        console.log("⚠️ gRPC detection unavailable (native binding missing - expected on Windows local dev):", err.message);
        console.log("   gRPC will work on Render (Linux). Falling back to WebSocket detection.");
      } else {
        console.error("❌ gRPC detection failed to start:", err.message);
      }
      createGrpcDetectionManager = null;
      grpcManager = null;
    }
  } else {
    console.log("⚠️ gRPC detection disabled (no GRPC_TOKEN set)");
  }

  rotateSubscriptions();
  setInterval(pollForMissedTrades, POLL_INTERVAL_MS);
  setInterval(checkWalletBalance, 60 * 1000);
  if (TELEGRAM_WEBHOOK_MODE) {
    await setupTelegramWebhook();
  } else {
    // Poll Telegram commands using long-polling; default every 30s interval
    setInterval(pollTelegramCommands, TELEGRAM_POLL_INTERVAL_MS);
  }

  // Probe and rank submission RPCs, then warm them periodically
  await probeSubmissionConnections().catch(() => {});
  startSubmissionWarmup();

  setInterval(() => {
    fetch("https://api.jup.ag/", { agent } as any).catch(() => {});
  }, 45_000);

  // Memory monitor - alert if heap exceeds 1.3GB (Node limit ~1.4GB with --max-old-space-size=1536)
  setInterval(() => {
    const used = process.memoryUsage().heapUsed / 1024 / 1024;
    if (used > 1200) {
      console.log(`\u{26A0} HIGH MEMORY: ${used.toFixed(0)} MB heap used`);
      sendTelegramAlert(`\u{26A0} <b>HIGH MEMORY</b>\n${used.toFixed(0)} MB heap used\nConsider restart if climbing.`);
    }
  }, 30_000);

  if (PUMPPORTAL_ENABLED) {
    console.log("BOT_STARTUP: starting PumpPortal");
    await startPumpPortal();
    console.log("BOT_STARTUP: PumpPortal connected");
  }

  await checkWalletBalance();

  const targetList = TARGET_WALLETS.map(w => `<code>${w.toString()}</code>`).join("\n");
  console.log("✅ Bot fully running (immediate copy-buy mode)");
  console.log(`TELEGRAM_EXTENDED=${TELEGRAM_EXTENDED} TELEGRAM_BOT_TOKEN=${TELEGRAM_BOT_TOKEN ? 'present' : 'missing'} TELEGRAM_CHAT_ID=${TELEGRAM_CHAT_ID ? 'present' : 'missing'}`);
  let channelsDesc = `${DETECTION_URLS.length} rotating WS + polling`;
  if (grpcManager) channelsDesc += " + gRPC";
  if (PENDING_SIGNATURE_ENABLED) channelsDesc += " + pending-signature feed";
  await sendTelegramAlert(
    `✅ <b>Bot Active</b>\nTargets:\n${targetList}\n` +
    `Buy: ${BUY_AMOUNT_SOL} SOL | Minimum copied buy: disabled\n` +
    `Buy path: PumpPortal + local Pump SDK (direct only)\n` +
    `Channels: ${channelsDesc}\n` +
    `Pre-buy MC filter: $${PRE_TRADE_MAX_MARKET_CAP_USD.toLocaleString()} max\n` +
    `Commands: /pause /resume /status${TELEGRAM_EXTENDED ? ' /positions /stats /history /settings' : ''}`
  );
}

start().catch((err) => {
  console.error("BOT_STARTUP FAILED:", err);
});

// Handle gRPC trade events
async function handleGrpcTradeEvent(event: GrpcTradeEvent) {
  try {
    console.log(`🚀 [gRPC] ${event.type.toUpperCase()} detected: ${event.mint} by ${event.user} (${event.solAmount / 1e9} SOL)`);

    if (event.type === "buy") {
      // Fire and forget - don't await executeBuy to minimize latency
      executeBuy(event.mint, undefined, event.solAmount, nowMs()).catch(e =>
        console.log("⚠️ executeBuy error (non-blocking):", e.message)
      );
    } else if (event.type === "sell") {
      // Check if we hold this token and target sold
      const held = positions.get(event.mint);
      if (held && held.remainingAmount > 0) {
        console.log("🚨 [gRPC] Target SOLD a token we hold — dumping:", event.mint);
        const sellAmount = targetExitSellAmount(held);
        if (sellAmount > 0) executeSell(event.mint, sellAmount, "TARGET_EXITED").catch(e =>
          console.log("⚠️ executeSell error (non-blocking):", e.message)
        );
      }
    }
  } catch (err: any) {
    console.error("[gRPC] Event handler error:", err.message);
  }
}
