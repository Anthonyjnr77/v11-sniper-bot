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
} from "@solana/web3.js";
import type { Logs } from "@solana/web3.js";
import { Program, AnchorProvider, BorshEventCoder } from "@coral-xyz/anchor";
import { getMint, getAssociatedTokenAddress, TOKEN_2022_PROGRAM_ID, TOKEN_PROGRAM_ID } from "@solana/spl-token";
import { OnlinePumpSdk, PUMP_SDK, getBuyTokenAmountFromSolAmount, bondingCurvePda } from "@pump-fun/pump-sdk";
import BN from "bn.js";
import fetch from "node-fetch";
import bs58 from "bs58";
import https from "https";
import express from "express";

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

function makeConnection(url: string): Connection {
  return new Connection(url, {
    commitment: "processed",
    wsEndpoint: url.replace("https", "wss"),
    httpAgent: agent,
  });
}

const connection = makeConnection(RPC_URL);

const WS_FANOUT = Math.max(1, Math.min(2, Number(process.env.WS_FANOUT ?? 2)));
const EXTRA_WS_URLS = (process.env.EXTRA_WS_URLS ?? "https://api.mainnet-beta.solana.com")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const DETECTION_URLS: string[] = [];
for (let i = 0; i < WS_FANOUT; i++) DETECTION_URLS.push(RPC_URL);
DETECTION_URLS.push(...EXTRA_WS_URLS);

const BROADCAST_RPC_URL = process.env.BROADCAST_RPC_URL ?? "https://api.mainnet-beta.solana.com";
const broadcastConnection = makeConnection(BROADCAST_RPC_URL);

const wallet = Keypair.fromSecretKey(bs58.decode(requireEnv("PRIVATE_KEY")));
const TARGET_WALLET = new PublicKey(requireEnv("TARGET_WALLET"));

const BUY_AMOUNT_SOL = Number(process.env.BUY_AMOUNT_SOL);
const BUY_AMOUNT = BUY_AMOUNT_SOL * 1e9;
const BASE_PRIORITY_FEE = Number(process.env.PRIORITY_FEE_LAMPORTS ?? 500_000);
const SLIPPAGE_BPS = Number(process.env.SLIPPAGE_BPS ?? 3000);
const RETRY_SLIPPAGE_BPS = Number(process.env.RETRY_SLIPPAGE_BPS ?? 5000);
const MIN_BUY_SOL = Number(process.env.MIN_BUY_SOL ?? 19) * 1e9;  // Only copy buys >= 19 SOL
const MAX_ENTRY_MARKET_CAP = Number(process.env.MAX_ENTRY_MARKET_CAP ?? 10000); // Don't buy if MC > 10k
const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS ?? 2500);

const MAX_PRICE_IMPACT = Number(process.env.MAX_PRICE_IMPACT ?? 0.15);
const ONLY_DIRECT_ROUTES = (process.env.ONLY_DIRECT_ROUTES ?? "true") === "true";
const PUMPPORTAL_ENABLED = (process.env.PUMPPORTAL ?? "true") === "true";

const MIN_WALLET_BALANCE_SOL = Number(process.env.MIN_WALLET_BALANCE_SOL ?? 0.02);

const SOL_MINT = "So11111111111111111111111111111111111111112";

const JUP_BASE = "https://api.jup.ag/swap/v1";
const JUP_API_KEY = process.env.JUP_API_KEY;

const TP_TIERS = [
  { tp: 7.0, sellFraction: 0.5 },
  { tp: 11.0, sellFraction: 0.3 },
  { tp: 15.0, sellFraction: 0.2 },
];

const TIME_BASED_TP = [
  { seconds: 15, sellFraction: 0.2 },
  { seconds: 25, sellFraction: 0.3 },
  { seconds: 40, sellFraction: 0.5 },
];
const TIME_BASED_TP_MAX_PNL = 3.0;

const SL_PCT = -0.35;
const TRAIL_DRAWDOWN = -0.2;
const MAX_HOLD_MS = 5 * 60 * 1000;

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
  timestamp: number;
}

const bondingCurveCache = new Map<string, CachedBondingCurve>();

// Pump.fun global PDA (constant, never changes)
const PUMPFUN_GLOBAL_PDA = new PublicKey("4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf");

/* ================= PRE-BUILD CACHE (Top Pump Tokens) ================= */
// Proactively fetch bonding curve data for top 500 Pump.fun tokens by volume
// so local SDK build is instant when target wallet buys them.
const PREBUILD_CACHE_TTL_MS = 10 * 60 * 1000; // 10 min
const PREBUILD_MAX_TOKENS = 500;
const prebuildCache = new Map<string, CachedBondingCurve>();

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
            prebuildCache.set(mint, {
              bondingCurve: bondingCurveAddress,
              associatedBondingCurve: associatedBondingCurve,
              virtualQuoteReserves: new BN(data.subarray(8, 16).readBigUInt64LE(0).toString()),
              virtualTokenReserves: new BN(data.subarray(16, 24).readBigUInt64LE(0).toString()),
              realQuoteReserves: new BN(data.subarray(24, 32).readBigUInt64LE(0).toString()),
              realTokenReserves: new BN(data.subarray(32, 40).readBigUInt64LE(0).toString()),
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
const pumpSdkConnection = new Connection(PUMP_SDK_RPC_URL, {
  commitment: "processed",
  wsEndpoint: PUMP_SDK_RPC_URL.replace("https", "wss"),
  httpAgent: agent,
});
const pumpSdkForBuild = new OnlinePumpSdk(pumpSdkConnection);

/* ================= DEDICATED BUY EXECUTION RPC ================= */
// Low-latency RPC for transaction submission only (no WS)
// Set BUY_EXEC_RPC_URL to a premium endpoint (Helius paid, Triton, QuickNode, etc.)
// Falls back to main RPC if not set.
const BUY_EXEC_RPC_URL = process.env.BUY_EXEC_RPC_URL ?? RPC_URL;
const buyExecConnection = new Connection(BUY_EXEC_RPC_URL, {
  commitment: "processed",
  httpAgent: agent,
  // Disable WS for this connection - we only need HTTP for sendRawTransaction
  wsEndpoint: "" as any,
});


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

/* ================= JITO ================= */

const JITO_BLOCK_ENGINE_URL =
  process.env.JITO_URL ?? "https://mainnet.block-engine.jito.wtf/api/v1/bundles";

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
const JITO_TIP_LAMPORTS = Math.floor(BUY_AMOUNT * JITO_TIP_BPS / 10000);

function randomTipAccount(): PublicKey {
  const idx = Math.floor(Math.random() * JITO_TIP_ACCOUNTS.length);
  const addr = JITO_TIP_ACCOUNTS[idx] ?? JITO_TIP_ACCOUNTS[0];
  return new PublicKey(addr as string);
}

function logFeeSizingWarning() {
  const currentPriorityFee = getDynamicPriorityFee();
  const totalFeeLamports = JITO_TIP_LAMPORTS + currentPriorityFee;
  const pctOfBuy = (totalFeeLamports / BUY_AMOUNT) * 100;
  console.log(
    `ðŸ’¸ Fee sizing: Jito tip + priority fee â‰ˆ ${(totalFeeLamports / 1e9).toFixed(4)} SOL ` +
    `(~${pctOfBuy.toFixed(1)}% of ${(BUY_AMOUNT / 1e9).toFixed(4)} SOL buy)`
  );
  console.log(
    `ðŸŽ¯ Effective slippage: ${(SLIPPAGE_BPS / 100).toFixed(0)}% (retry ${(RETRY_SLIPPAGE_BPS / 100).toFixed(0)}%) â€” ` +
    `if this is not 30/50, a SLIPPAGE_BPS env var on Render is overriding the code`
  );
}

/* ================= NETWORK ================= */

async function fetchJson(url: string, options: any = {}, retries = 2): Promise<any> {
  for (let i = 0; i <= retries; i++) {
    try {
      return await fetch(url, { agent, ...options }).then((r: any) => r.json());
    } catch (e: any) {
      if (i === retries) throw e;
      await new Promise((r) => setTimeout(r, 300));
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

async function refreshBlockhash() {
  try {
    // Use buyExecConnection for faster response
    const { blockhash } = await buyExecConnection.getLatestBlockhash("processed");
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
  return (await buyExecConnection.getLatestBlockhash("processed")).blockhash;
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
  tiersHit: boolean[];
  timeTiersHit: boolean[];
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

async function pollTelegramCommands() {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;
  try {
    const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/getUpdates?offset=${telegramUpdateOffset}&timeout=0`;
    const data: any = await fetchJson(url);
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

      const text = (msg.text ?? "").trim().toLowerCase();
      if (text === "/pause") {
        botPaused = true;
        console.log("\u{23F8} Bot PAUSED via Telegram command");
        await sendTelegramAlert("\u{23F8} <b>Bot paused.</b> No new buys will be taken. Existing positions still monitored/sold normally. Send /resume to continue.");
      } else if (text === "/resume") {
        botPaused = false;
        console.log("\u{25B6} Bot RESUMED via Telegram command");
        await sendTelegramAlert("\u{25B6} <b>Bot resumed.</b> New buys re-enabled.");
      } else if (text === "/status") {
        const balanceStr = lastKnownBalanceLamports !== null
          ? `${(lastKnownBalanceLamports / 1e9).toFixed(4)} SOL`
          : "unknown";
        await sendTelegramAlert(
          `\u{1F4E2} <b>Status</b>\n` +
          `Paused: ${botPaused ? "YES" : "no"}\n` +
          `Open positions: ${positions.size}\n` +
          `Wallet: ${balanceStr}\n` +
          `WS channels: ${DETECTION_URLS.length}\n` +
          `PumpPortal: ${pumpPortalConnected ? "\u{2705} connected" : "\u{274C} NOT connected"}`
        );
      }
    }
  } catch (e: any) {
    console.log("Telegram poll error:", e.message);
  }
}

/* ================= JUPITER (fallback path) ================= */

async function getQuote(inputMint: string, outputMint: string, amount: number, attempts = 4) {
  let lastError: string | null = null;
  for (let i = 0; i < attempts; i++) {
    const useDirect = ONLY_DIRECT_ROUTES && i < 2;
    let url = `${JUP_BASE}/quote?inputMint=${inputMint}&outputMint=${outputMint}&amount=${Math.floor(
      amount
    )}&slippageBps=${SLIPPAGE_BPS}`;
    if (useDirect) url += "&onlyDirectRoutes=true";
    try {
      const data = await fetchJson(url, { headers: jupHeaders() });
      if (data && !data.error && data.outAmount) return data;
      lastError = data?.error ? String(data.error) : "empty quote response";
    } catch (e: any) {
      lastError = e.message;
    }
    if (i < attempts - 1) await new Promise((r) => setTimeout(r, 400));
  }
  if (lastError && attempts > 1) {
    console.log("âŒ Quote failed after retries:", String(lastError).slice(0, 140));
  }
  return null;
}

async function buildSwapTx(quoteResponse: any) {
  const data = await fetchJson(`${JUP_BASE}/swap`, {
    method: "POST",
    headers: jupHeaders({ "Content-Type": "application/json" }),
    body: JSON.stringify({
      quoteResponse,
      userPublicKey: wallet.publicKey.toString(),
      wrapAndUnwrapSol: true,
      prioritizationFeeLamports: getPriorityFee(),
      dynamicComputeUnitLimit: false,
    }),
  });
  if (!data || data.error || !data.swapTransaction) return null;
  return data.swapTransaction;
}

/* ================= DUAL-PATH SUBMISSION ================= */

// Multi-RPC endpoints for parallel submission (first success wins)
const SUBMISSION_RPCS = [
  process.env.BUY_EXEC_RPC_URL,
  process.env.RPC_URL,
  "https://rpc.ankr.com/solana",
  "https://solana-rpc.publicnode.com",
  "https://api.mainnet-beta.solana.com",
].filter((v): v is string => Boolean(v));

const submissionConnections = SUBMISSION_RPCS.map(url =>
  new Connection(url, { commitment: "processed", httpAgent: agent })
);

// Dynamic priority fee cache (refreshed every 15s)
let dynamicPriorityFeeLamports = BASE_PRIORITY_FEE;
let lastPriorityFeeUpdate = 0;
const PRIORITY_FEE_TTL_MS = 15_000;

async function refreshPriorityFee() {
  try {
    // Fetch recent prioritization fees from multiple RPCs, use 95th percentile
    const fees = await Promise.allSettled(
      submissionConnections.slice(0, 3).map(c => c.getRecentPrioritizationFees())
    );
    const allFees: number[] = [];
    for (const f of fees) {
      if (f.status === "fulfilled" && f.value) {
        allFees.push(...f.value.map(x => x.prioritizationFee));
      }
    }
    if (allFees.length === 0) return;
    allFees.sort((a, b) => a - b);
    const p95 = allFees[Math.floor(allFees.length * 0.95)] ?? BASE_PRIORITY_FEE;
    // Add 20% buffer, cap at 5x base
    dynamicPriorityFeeLamports = Math.min(Math.ceil(p95 * 1.2), BASE_PRIORITY_FEE * 5);
    lastPriorityFeeUpdate = Date.now();
    console.log(`âš™ï¸ Dynamic priority fee updated: ${dynamicPriorityFeeLamports} lamports (P95: ${p95})`);
  } catch (e: any) {
    console.log("âš ï¸ Priority fee refresh failed:", e.message);
  }
}

// Refresh priority fee periodically
setInterval(refreshPriorityFee, PRIORITY_FEE_TTL_MS);
// Initial fetch
refreshPriorityFee();

function getDynamicPriorityFee(): number {
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

async function submitSignedTransaction(rawBytes: Uint8Array, useJito = BUY_AMOUNT >= 0.1 * 1e9): Promise<string | null> {
  const signed = VersionedTransaction.deserialize(rawBytes);
  const signature = bs58.encode(signed.signatures[0]!);

  // Parallel multi-RPC submit: fire to all endpoints simultaneously, first success wins
  const rpcAttempts = submissionConnections.map(conn =>
    conn.sendRawTransaction(rawBytes, { skipPreflight: true, maxRetries: 0 })
      .then(() => "RPC")
      .catch(() => null)
  );

  // Only use Jito for larger buys where MEV protection matters
  let jitoAttempt: Promise<string | null>;
  if (useJito) {
    jitoAttempt = (async () => {
      const blockhash = await getBlockhashFast();
      const tipTx = new Transaction({ feePayer: wallet.publicKey, recentBlockhash: blockhash }).add(
        SystemProgram.transfer({ fromPubkey: wallet.publicKey, toPubkey: randomTipAccount(), lamports: JITO_TIP_LAMPORTS })
      );
      tipTx.sign(wallet);
      const res = await fetchJson(JITO_BLOCK_ENGINE_URL, {
        method: "POST", headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "sendBundle", params: [[bs58.encode(rawBytes), bs58.encode(tipTx.serialize())]] }),
      });
      if (!res?.result) throw new Error("Jito did not accept the bundle");
      return "Jito";
    })();
  } else {
    jitoAttempt = Promise.resolve(null);
  }

  scheduleRebroadcasts(rawBytes);
  try {
    // Race all RPC attempts + Jito, first non-null wins
    const allAttempts = [...rpcAttempts, jitoAttempt];
    const via = await Promise.race(allAttempts.filter(p => p !== null));
    if (!via) throw new Error("All submission paths failed");
    console.log(`ðŸ“¤ Transaction submitted via ${via}:`, signature);
    return signature;
  } catch {
    // Final fallback: retry on dedicated RPC with retries
    try {
      await buyExecConnection.sendRawTransaction(rawBytes, { skipPreflight: true, maxRetries: 2 });
      console.log(`ðŸ“¤ Transaction submitted via RPC (retry):`, signature);
      return signature;
    } catch { return null; }
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

 /* ================= PUMPPORTAL-BUILT TRANSACTIONS ================= */
 // Build direct Pump.fun buys locally from the official SDK. This avoids the
 // PumpPortal HTTP hop and uses the current bonding-curve accounts for the mint.
 async function buildLocalPumpBuyTx(mint: string, slippageBps = SLIPPAGE_BPS): Promise<Uint8Array | null> {
   try {
     const mintKey = new PublicKey(mint);
      const latest = await pumpSdkConnection.getLatestBlockhash("processed");
      const [mintInfo, global] = await Promise.all([
        pumpSdkConnection.getAccountInfo(mintKey, "processed"),
        pumpSdkForBuild.fetchGlobal(), // dedicated SDK RPC
      ]);
      if (!mintInfo) throw new Error("mint account not found");
     const tokenProgram = mintInfo.owner;
     if (!tokenProgram.equals(TOKEN_PROGRAM_ID) && !tokenProgram.equals(TOKEN_2022_PROGRAM_ID)) {
       throw new Error("unsupported token program");
     }

      // Check bonding curve cache first to avoid "zero tokens" on fresh tokens
      let cachedCurve = bondingCurveCache.get(mint);
      if (cachedCurve && Date.now() - cachedCurve.timestamp < BONDING_CURVE_TTL_MS) {
        console.log("âš¡ Using cached bonding curve for", mint);
      } else {
        // Fall back to pre-build cache for top tokens
        const prebuilt = prebuildCache.get(mint);
        if (prebuilt && Date.now() - prebuilt.timestamp < PREBUILD_CACHE_TTL_MS) {
          console.log("ðŸ§¨ Using pre-built bonding curve for", mint);
          cachedCurve = prebuilt;
        } else {
          cachedCurve = undefined;
        }
      }

      const [buyState, mintState, feeConfig] = await Promise.all([
        pumpSdkForBuild.fetchBuyState(mintKey, wallet.publicKey, tokenProgram), // dedicated SDK RPC
        getMint(pumpSdkConnection, mintKey, "processed", tokenProgram), // dedicated SDK RPC
        pumpSdkForBuild.fetchFeeConfig(), // dedicated SDK RPC
      ]);
     const solAmount = new BN(BUY_AMOUNT);

      // buyState.bondingCurve is the bonding curve account address (PublicKey).
      // getBuyTokenAmountFromSolAmount requires a BondingCurve object with reserves.
      // If we have cached reserves (from PumpPortal), use them. Otherwise fetch and parse the account.
      let bondingCurveData: any;
      if (cachedCurve) {
        bondingCurveData = {
          virtualTokenReserves: cachedCurve.virtualTokenReserves,
          virtualQuoteReserves: cachedCurve.virtualQuoteReserves,
          realTokenReserves: cachedCurve.realTokenReserves,
          realQuoteReserves: cachedCurve.realQuoteReserves,
          tokenTotalSupply: (typeof mintState.supply === "bigint" ? new BN(mintState.supply.toString()) : mintState.supply),
          complete: false,
          creator: new PublicKey("11111111111111111111111111111111"),
          isMayhemMode: false,
          isCashbackCoin: false,
          quoteMint: new PublicKey(SOL_MINT),
        };
      } else {
        // Fetch bonding curve account data and parse reserves
        const bondingCurveAddress = bondingCurvePda(mintKey);
        const bcAccount = await pumpSdkConnection.getAccountInfo(bondingCurveAddress, "processed");
        if (!bcAccount || bcAccount.data.length < 8 + 8 + 8 + 8 + 8 + 8) {
          throw new Error("Failed to fetch bonding curve account data");
        }
        const data = bcAccount.data;
        // Pump.fun bonding curve layout: discriminator(8) + virtualSolReserves(8) + virtualTokenReserves(8) + realSolReserves(8) + realTokenReserves(8) + totalSupply(8)
        bondingCurveData = {
          virtualTokenReserves: new BN(data.subarray(16, 24).readBigUInt64LE(0).toString()),
          virtualQuoteReserves: new BN(data.subarray(8, 16).readBigUInt64LE(0).toString()),
          realTokenReserves: new BN(data.subarray(32, 40).readBigUInt64LE(0).toString()),
          realQuoteReserves: new BN(data.subarray(24, 32).readBigUInt64LE(0).toString()),
          tokenTotalSupply: (typeof mintState.supply === "bigint" ? new BN(mintState.supply.toString()) : mintState.supply),
          complete: data[48] === 1,
          creator: new PublicKey("11111111111111111111111111111111"),
          isMayhemMode: false,
          isCashbackCoin: false,
          quoteMint: new PublicKey(SOL_MINT),
        };
      }
     const tokenAmount = getBuyTokenAmountFromSolAmount({
       global,
       feeConfig,
       mintSupply: (typeof mintState.supply === "bigint" ? new BN(mintState.supply.toString()) : mintState.supply),
       bondingCurve: bondingCurveData,
       amount: solAmount,
       quoteMint: new PublicKey(SOL_MINT),
     });
     if (tokenAmount.isZero()) throw new Error("bonding curve returned zero tokens");

     const instructions = await PUMP_SDK.buyInstructions({
       global,
        ...buyState, // buyState already has bondingCurve, associatedBondingCurve, etc.
       mint: mintKey,
       user: wallet.publicKey,
       amount: tokenAmount,
       solAmount,
       slippage: slippageBps / 100,
       tokenProgram,
     });
     const dynamicFee = getDynamicPriorityFee();
    const priorityMicroLamports = Math.ceil((dynamicFee * 1_000_000) / LOCAL_PUMP_COMPUTE_UNITS);
     const message = new TransactionMessage({
       payerKey: wallet.publicKey,
        recentBlockhash: latest.blockhash,
       instructions: [
         ComputeBudgetProgram.setComputeUnitLimit({ units: LOCAL_PUMP_COMPUTE_UNITS }),
         ComputeBudgetProgram.setComputeUnitPrice({ microLamports: priorityMicroLamports }),
         ...instructions,
       ],
     }).compileToV0Message();
     const tx = new VersionedTransaction(message);
     tx.sign([wallet]);
     return tx.serialize();
   } catch (e: any) {
     console.log("â†ªï¸ Local Pump.fun build unavailable:", e.message);
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
  try {
    const res: any = await fetch("https://pumpportal.fun/api/trade-local", {
      agent,
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        publicKey: wallet.publicKey.toString(),
        action,
        mint,
        amount,
        denominatedInSol: denominatedInSol ? "true" : "false",
        slippage: slippageBps / 100,
        priorityFee: getDynamicPriorityFee() / 1e9,
        pool,
      }),
    });
    if (!res.ok) {
      const errText = await res.text().catch(() => "");
      console.log(`âš ï¸ trade-local ${action} build failed:`, String(errText).slice(0, 140));
      return null;
    }
    const buf = new Uint8Array(await res.arrayBuffer());
    const tx = VersionedTransaction.deserialize(buf);
    tx.sign([wallet]);
    return tx.serialize();
  } catch (e: any) {
    console.log(`âš ï¸ trade-local ${action} error:`, e.message);
    return null;
  }
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
          return "failed";
        }
        if (status.confirmationStatus === "confirmed" || status.confirmationStatus === "finalized") return "confirmed";
      }
    } catch {}
    if (i < attempts - 1) await new Promise((r) => setTimeout(r, delayMs));
  }
  return "pending";
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
  if (!quote || !passesFilters(quote)) return null;
  const txBase64 = await buildSwapTx(quote);
  if (!txBase64) return null;
  const swapTx = VersionedTransaction.deserialize(Buffer.from(txBase64, "base64"));
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
    targetSnapshotPromise?: Promise<{ tokenPriceUSD: number | null; marketCapUSD: number | null }>
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

    const requiredLamports = BUY_AMOUNT + JITO_TIP_LAMPORTS + 3_000_000;
    if (lastKnownBalanceLamports !== null && lastKnownBalanceLamports < requiredLamports) {
      console.log(
        `\⏸ Buy skipped — wallet ${(lastKnownBalanceLamports / 1e9).toFixed(4)} SOL below required ~${(requiredLamports / 1e9).toFixed(4)} SOL:`,
        mint
      );
      inFlight.delete(mint);
      return;
    }

    // Market cap filter - skip if entry MC > MAX_ENTRY_MARKET_CAP
    try {
      const mcSnapshot = await delayedMarketCapSnapshot(mint, 0);
      const targetMC = mcSnapshot.marketCapUSD;
      if (targetMC && targetMC > MAX_ENTRY_MARKET_CAP) {
        console.log(`⏸ Buy skipped — entry MC $${targetMC.toLocaleString()} > ${MAX_ENTRY_MARKET_CAP}:`, mint);
        inFlight.delete(mint);
        return;
      }
      console.log(`✅ MC check passed: $${(targetMC ?? 0).toLocaleString()} for ${mint}`);
    } catch (e: any) {
      console.log("⚠️ MC check failed, proceeding anyway:", e.message);
    }

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
        // ===== PHASE 2: PARALLEL BUILDER RACE =====
        // Fire all builders simultaneously; first valid signed tx wins.
        // This eliminates the sequential wait that was adding 300-800ms latency.
        async function buildAndSend(builderName: string, buildFn: () => Promise<Uint8Array | null>): Promise<string | null> {
          try {
            const tx = await buildFn();
            if (tx) {
              const s = await sendRawTransactionDual(tx);
              if (s) {
                console.log(`ðŸš€ BUY submitted (${builderName}):`, mint, s);
                return s;
              }
            }
          } catch (e: any) {
            console.log(`âš ï¸ ${builderName} build error:`, e.message);
          }
          return null;
        }

        const [sigPumpPortal, sigLocal, sigJupiter] = await Promise.allSettled([
          buildAndSend("PumpPortal-trade-local", () => buildPumpPortalTx("buy", mint, BUY_AMOUNT / 1e9, true, SLIPPAGE_BPS, "pump")),
          buildAndSend("local Pump SDK",     () => buildLocalPumpBuyTx(mint, SLIPPAGE_BPS)),
          buildAndSend("Jupiter",            () => buildJupiterBuyTx(mint)),
        ]);

        sig = (sigPumpPortal.status === "fulfilled" ? sigPumpPortal.value : null) ?? (sigLocal.status === "fulfilled" ? sigLocal.value : null) ?? (sigJupiter.status === "fulfilled" ? sigJupiter.value : null) ?? null;

        if (!sig) {
          console.log("âŒ Buy aborted — all builders failed for", mint);
          return;
        }
      }

      const myMcPromise = delayedMarketCapSnapshot(mint, 2000);
      const targetMcPromise = targetSnapshotPromise ?? Promise.resolve({ tokenPriceUSD: null, marketCapUSD: null });
      let confirmationPromise = waitForConfirmation(sig);
      let actual = await waitForBalance(mint);
      let confirmation = await confirmationPromise;

      if (confirmation === "failed") {
        // Rebuild ONCE with completely fresh state at higher slippage. Local
        // SDK first (derived accounts, current price), then PumpPortal.
        // Jupiter last — it usually cannot even quote a seconds-old token.
        console.log(`↪️ Buy failed on-chain — rebuilding fresh at ${RETRY_SLIPPAGE_BPS / 100}% slippage:`, mint);
      const failedPrimarySig = sig;
      sig = null;
      let retryTx = await buildLocalPumpBuyTx(mint, RETRY_SLIPPAGE_BPS);
      if (!retryTx) retryTx = await buildPumpPortalTx("buy", mint, BUY_AMOUNT / 1e9, true, RETRY_SLIPPAGE_BPS, "pump");
      if (!retryTx) retryTx = await buildPumpPortalTx("buy", mint, BUY_AMOUNT / 1e9, true, RETRY_SLIPPAGE_BPS, "auto");
      if (retryTx) {
        sig = await sendRawTransactionDual(retryTx);
        if (sig) console.log("ðŸš€ BUY retry submitted (fresh build, higher slippage):", mint, sig);
      }
      if (!sig) {
        console.log("â†ªï¸ Rebuild unavailable â€” trying Jupiter for", mint);
        sig = await submitJupiterBuy(mint);
      }
      if (!sig) {
        await sendTelegramAlert(
          `❌ <b>BUY FAILED ON-CHAIN</b>\nMint: <code>${mint}</code>\n` +
          `Primary tx failed; rebuild and Jupiter fallback were unavailable.\n` +
          `<a href="https://solscan.io/tx/${failedPrimarySig}">View failed transaction</a>`
        );
        return;
      }
      confirmationPromise = waitForConfirmation(sig);
      actual = await waitForBalance(mint);
      confirmation = await confirmationPromise;
      if (confirmation === "failed") {
        await sendTelegramAlert(
          `❌ <b>BUY FAILED ON-CHAIN</b>\nMint: <code>${mint}</code>\n` +
          `Retry also failed — the exact program error is in the Render logs.\n` +
          `<a href="https://solscan.io/tx/${sig}">View transaction</a>`
        );
        return;
      }
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
      tiersHit: TP_TIERS.map(() => false),
      timeTiersHit: TIME_BASED_TP.map(() => false),
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

/* ================= MONITOR ================= */

async function monitor() {
  for (const [mint, pos] of positions.entries()) {
    if (pos.remainingAmount <= 0) continue;

    const elapsedSeconds = (Date.now() - pos.entryTime) / 1000;

    getQuote(mint, SOL_MINT, pos.remainingAmount, 1).then((quote) => {
      if (!quote) return;

      const value = Number(quote.outAmount);
      const pnl = (value - pos.costBasisLamports) / pos.costBasisLamports;

      if (pnl <= SL_PCT) {
        recordStopLoss();
        executeSell(mint, pos.remainingAmount, `SL ${(pnl * 100).toFixed(0)}%`);
        return;
      }
      if (Date.now() - pos.entryTime > MAX_HOLD_MS) {
        executeSell(mint, pos.remainingAmount, "MAX_TIME");
        return;
      }
      if (value > pos.costBasisLamports) {
        if (value > pos.highestValue) pos.highestValue = value;
        const drawdown = (value - pos.highestValue) / pos.highestValue;
        if (pos.highestValue > pos.costBasisLamports && drawdown < TRAIL_DRAWDOWN) {
          executeSell(mint, pos.remainingAmount, "TRAILING_STOP");
          return;
        }
      }

      if (pnl < TIME_BASED_TP_MAX_PNL) {
        for (let i = 0; i < TIME_BASED_TP.length; i++) {
          if (pos.timeTiersHit[i]) continue;
          const tier = TIME_BASED_TP[i];
          if (!tier) continue;
          if (elapsedSeconds >= tier.seconds) {
            const sellAmount = Math.min(
              Math.floor(pos.originalAmount * tier.sellFraction),
              pos.remainingAmount
            );
            if (sellAmount > 0) {
              executeSell(mint, sellAmount, `TIME-${tier.seconds}s (PnL: ${(pnl * 100).toFixed(0)}%)`);
              pos.timeTiersHit[i] = true;
            }
          }
        }
      }

      for (let i = 0; i < TP_TIERS.length; i++) {
        if (pos.tiersHit[i]) continue;
        const tier = TP_TIERS[i];
        if (!tier) continue;
        if (pnl >= tier.tp) {
          const sellAmount = Math.min(
            Math.floor(pos.originalAmount * tier.sellFraction),
            pos.remainingAmount
          );
          if (sellAmount > 0) {
            executeSell(mint, sellAmount, `TP${i + 1} +${(tier.tp * 100).toFixed(0)}%`);
            pos.tiersHit[i] = true;
          }
        }
      }
    });
  }
}

setInterval(monitor, 1500);

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
    const targetIdx = keys.findIndex((k) => k.equals(TARGET_WALLET));
    if (targetIdx === -1) return false;

    const preTok = tx.meta.preTokenBalances ?? [];
    const postTok = tx.meta.postTokenBalances ?? [];
    for (const pre of preTok) {
      if (pre.owner !== TARGET_WALLET.toString()) continue;
      if (!positions.has(pre.mint)) continue;
      const post = postTok.find((p: any) => p.owner === pre.owner && p.mint === pre.mint);
      const preAmount = Number(pre.uiTokenAmount.uiAmount ?? 0);
      const postAmount = post ? Number(post.uiTokenAmount.uiAmount ?? 0) : 0;
      if (postAmount < preAmount) {
        const held = positions.get(pre.mint);
        if (held && held.remainingAmount > 0) {
          console.log("ðŸš¨ TARGET EXITED â€” dumping:", pre.mint);
          executeSell(pre.mint, held.remainingAmount, "TARGET_EXITED");
          return true;
        }
      }
    }

    const preBalances = tx.meta.preTokenBalances ?? [];
    const postBalances = tx.meta.postTokenBalances ?? [];
    let foundAny = false;
    for (const post of postBalances) {
      if (post.owner !== TARGET_WALLET.toString()) continue;
      if (post.mint === SOL_MINT) continue;
      const pre = preBalances.find((p: any) => p.owner === post.owner && p.mint === post.mint);
      const preAmount = pre ? Number(pre.uiTokenAmount.uiAmount) : 0;
      const postAmount = Number(post.uiTokenAmount.uiAmount);
      if (postAmount > preAmount) {
        foundAny = true;
        console.log("ðŸ”¥ DETECTED buy:", post.mint);
        executeBuy(post.mint, delayedMarketCapSnapshot(post.mint));
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

  console.log(`ðŸ‘€ Activity: https://solscan.io/tx/${signature}`);

  const fast = tryFastDecode(log.logs);
  const isTargetTrade = fast && (() => {
    try { return new PublicKey(fast.user).equals(TARGET_WALLET); } catch { return false; }
  })();

  if (isTargetTrade && fast.isBuy && fast.solAmount >= MIN_BUY_SOL) {
    console.log(`âš¡ IDL FAST-PATH BUY: ${fast.mint}`);
    seenSignatures.add(signature);
    // Fire and forget - don't await executeBuy to minimize latency
    executeBuy(fast.mint, delayedMarketCapSnapshot(fast.mint)).catch(e =>
      console.log("âš ï¸ executeBuy error (non-blocking):", e.message)
    );
    return;
  }
  if (isTargetTrade && !fast.isBuy) {
    const held = positions.get(fast.mint);
    if (held && held.remainingAmount > 0) {
      console.log("ðŸš¨ Fast-path: target SOLD a token we hold â€” dumping:", fast.mint);
      seenSignatures.add(signature);
      executeSell(fast.mint, held.remainingAmount, "TARGET_EXITED").catch(e =>
        console.log("âš ï¸ executeSell error (non-blocking):", e.message)
      );
      return;
    }
  }

  if (fast && !isTargetTrade) {
    console.log("â†ªï¸ Pump.fun trade event belongs to another wallet â€” racing RPC transaction fetch");
  } else if (fast && fast.isBuy && fast.solAmount < MIN_BUY_SOL) {
    console.log("â†ªï¸ Pump.fun buy is below MIN_BUY_SOL â€” racing RPC transaction fetch");
  } else {
    console.log("â†ªï¸ No direct Pump.fun TradeEvent â€” racing RPC transaction fetch");
  }
  handleTx(signature);
}

interface DetectionChannel {
  conn: Connection;
  subId: number;
}

let activeChannels: DetectionChannel[] = [];

function rotateSubscriptions() {
  const fresh: DetectionChannel[] = [];
  for (const url of DETECTION_URLS) {
    try {
      const conn = makeConnection(url);
      const subId = conn.onLogs(TARGET_WALLET, onWalletLog, "processed");
      fresh.push({ conn, subId });
    } catch (e: any) {
      console.log("âš ï¸ Failed to open detection channel:", e.message);
    }
  }

  if (fresh.length === 0) {
    console.log("âš ï¸ Rotation produced no channels â€” keeping the old ones alive");
    return;
  }

  // Immediately clean up old channels to prevent memory leaks
  for (const ch of activeChannels) {
    try {
      ch.conn.removeOnLogsListener(ch.subId);
    } catch {}
    try {
      // Force close the underlying WebSocket
      (ch.conn as any)._rpcWebSocket?.close?.();
    } catch {}
  }

  activeChannels = fresh;
  console.log(`âœ… ${fresh.length} detection channel(s) rotated onto fresh websockets`);
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

  const connect = () => {
    let ws: any;
    try {
      // Validate WS is a callable constructor
      if (typeof WS !== "function") {
        console.log("âš ï¸ PumpPortal: WS is not a function, skipping reconnect");
        setTimeout(connect, 30000);
        return;
      }
      ws = new WS(PUMPPORTAL_WS);
    } catch (e: any) {
      console.log("âš ï¸ PumpPortal WS creation failed:", e.message);
      setTimeout(connect, 5000);
      return;
    }

    if (!ws) {
      console.log("âš ï¸ PumpPortal WS returned null/undefined, retrying in 5s");
      setTimeout(connect, 5000);
      return;
    }

    ws.onopen = () => {
      pumpPortalConnected = true;
      console.log("âœ… PumpPortal channel connected");
      ws.send(JSON.stringify({ method: "subscribeAccountTrade", keys: [TARGET_WALLET.toString()] }));
    };

     ws.onmessage = (ev: any) => {
       try {
         const raw = typeof ev.data === "string" ? ev.data : ev.data.toString();
         const msg = JSON.parse(raw);
         if (!msg?.signature || !msg?.mint || !msg.traderPublicKey) return;

         if (msg.traderPublicKey !== TARGET_WALLET.toString()) return;
         if (seenSignatures.has(msg.signature)) return;

         if (msg.txType === "sell") {
           const held = positions.get(msg.mint);
           if (held && held.remainingAmount > 0) {
             console.log("ðŸš¨ PumpPortal: target SOLD a token we hold â€” dumping:", msg.mint);
             seenSignatures.add(msg.signature);
             executeSell(msg.mint, held.remainingAmount, "TARGET_EXITED");
           }
           return;
         }
         if (msg.txType !== "buy") return;

         const lamports = Number(msg.solAmount ?? 0) * 1e9;
         if (lamports < MIN_BUY_SOL) {
           console.log(`â­ï¸ PumpPortal: buy below threshold (${Number(msg.solAmount).toFixed(4)} SOL) â€” ignoring`);
           return;
         }

         console.log("âš¡ PUMPPORTAL detected buy:", msg.mint);
         seenSignatures.add(msg.signature);

          // Prime bonding curve cache from PumpPortal event (if present)
          // PumpPortal sometimes includes bonding curve account in the trade event
          if (msg.bondingCurve && msg.associatedBondingCurve) {
            try {
              const bondingCurveKey = new PublicKey(msg.bondingCurve);
              const associatedBondingCurveKey = new PublicKey(msg.associatedBondingCurve);
              // Fetch and cache asynchronously, don't block the hot path
              (async () => {
                try {
                  const [bcAccount, abcAccount, feeConfig] = await Promise.all([
                    pumpSdkConnection.getAccountInfo(bondingCurveKey, "processed"),
                    pumpSdkConnection.getAccountInfo(associatedBondingCurveKey, "processed"),
                    pumpSdkForBuild.fetchFeeConfig(),
                  ]);
                  if (bcAccount && abcAccount && feeConfig) {
                    // Parse bonding curve state (ASCII layout from Pump.fun)
                    const data = bcAccount.data;
                    if (data.length >= 8 + 8 + 8 + 8 + 8 + 8) {
                      // Discriminator (8) + virtualQuoteReserves (8) + virtualTokenReserves (8) + realQuoteReserves (8) + realTokenReserves (8) + totalSupply (8)
                      const virtualQuoteReserves = new BN(data.subarray(8, 16).readBigUInt64LE(0).toString());
                      const virtualTokenReserves = new BN(data.subarray(16, 24).readBigUInt64LE(0).toString());
                      const realQuoteReserves = new BN(data.subarray(24, 32).readBigUInt64LE(0).toString());
                      const realTokenReserves = new BN(data.subarray(32, 40).readBigUInt64LE(0).toString());
                      bondingCurveCache.set(msg.mint, {
                        bondingCurve: bondingCurveKey,
                        associatedBondingCurve: associatedBondingCurveKey,
                        virtualQuoteReserves,
                        virtualTokenReserves,
                        realQuoteReserves,
                        realTokenReserves,
                        timestamp: Date.now(),
                      });
                      console.log("ðŸ’¾ Cached bonding curve for", msg.mint);
                    }
                  }
                } catch (e: any) {
                  console.log("âš ï¸ Bonding curve cache prime failed:", e.message);
                }
              })();
            } catch {}
          }

         executeBuy(msg.mint, delayedMarketCapSnapshot(msg.mint)).catch(e =>
            console.log("âš ï¸ executeBuy error (non-blocking):", e.message)
          );
       } catch {
         // non-JSON frame, ignore
       }
     };

    ws.onclose = () => {
      pumpPortalConnected = false;
      console.log("âš ï¸ PumpPortal channel closed â€” reconnecting in 3s");
      setTimeout(connect, 3000);
    };

    ws.onerror = (err: any) => {
      console.log("âš ï¸ PumpPortal WS error:", err?.message ?? String(err));
      try {
        ws.close();
      } catch {}
    };
  };

  connect();
}

/* ================= POLLING RECONCILIATION LOOP ================= */

let lastPolledSignature: string | null = null;

async function initPollingCursor() {
  try {
    const sigs = await connection.getSignaturesForAddress(TARGET_WALLET, { limit: 1 }, "confirmed");
    if (sigs.length > 0 && sigs[0]) {
      lastPolledSignature = sigs[0].signature;
      console.log("âœ… Polling cursor initialized at:", lastPolledSignature);
    }
  } catch (e: any) {
    console.log("âš ï¸ Could not initialize polling cursor:", e.message);
  }
}

async function pollForMissedTrades() {
  try {
    const options: any = { limit: 25 };
    if (lastPolledSignature) options.until = lastPolledSignature;

    const sigs = await connection.getSignaturesForAddress(TARGET_WALLET, options, "confirmed");
    if (sigs.length === 0) return;

    const ordered = [...sigs].reverse();

    for (const sigInfo of ordered) {
      if (sigInfo.err) continue;
      if (seenSignatures.has(sigInfo.signature)) continue;
      console.log("ðŸ”Ž POLL found unprocessed signature:", sigInfo.signature);
      handleTx(sigInfo.signature);
    }

    lastPolledSignature = sigs[0]?.signature ?? lastPolledSignature;
  } catch (e: any) {
    console.log("Polling error:", e.message);
  }
}

/* ================= KEEP-ALIVE SERVER ================= */

const app = express();
app.get("/", (_req, res) => res.send("Engine Active"));
app.get("/health", (_req, res) => res.json({
  status: "ok",
  positions: positions.size,
  paused: botPaused,
  pumpPortal: pumpPortalConnected,
  uptime: process.uptime(),
}));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Keep-alive server on port ${PORT}`));

/* ================= START ================= */

async function start() {
  console.log("ðŸš€ Copy-trade bot starting. Target:", TARGET_WALLET.toString());

  logFeeSizingWarning();

  await Promise.all([loadPositions(), initPumpDecoder(), initPollingCursor()]);

  // Start background blockhash refresher (replaces old 25s interval)
  startBlockhashRefresher();

  rotateSubscriptions();
  setInterval(rotateSubscriptions, 60 * 1000);
  setInterval(pollForMissedTrades, POLL_INTERVAL_MS);
  setInterval(checkWalletBalance, 60 * 1000);
  setInterval(pollTelegramCommands, 3000);

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

  if (PUMPPORTAL_ENABLED) await startPumpPortal();

  await checkWalletBalance();

  console.log("✅ Bot fully running (rotating WS + polling + Pump SDK buys)");
  await sendTelegramAlert(
    `✅ <b>Bot Active</b>\nTarget: <code>${TARGET_WALLET.toString()}</code>\n` +
    `Buy: ${BUY_AMOUNT_SOL} SOL | Min copied buy: ${MIN_BUY_SOL / 1e9} SOL | Max entry MC: $${MAX_ENTRY_MARKET_CAP.toLocaleString()}\n` +
    `Buy path: Pump SDK → Jupiter\n` +
    `Channels: ${DETECTION_URLS.length} rotating WS + polling\n` +
    `Filters: min 19 SOL buy, max 10k MC entry\n` +
    `Commands: /pause /resume /status`
  );
}

start();



















