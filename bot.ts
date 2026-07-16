import "dotenv/config";
import {
  Connection,
  Keypair,
  VersionedTransaction,
  PublicKey,
  SystemProgram,
  Transaction,
} from "@solana/web3.js";
import { Program, AnchorProvider, BorshEventCoder } from "@coral-xyz/anchor";
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
const connection = new Connection(RPC_URL, {
  commitment: "confirmed", // Upgraded to confirmed for fallback safety
  wsEndpoint: RPC_URL.replace("https", "wss"),
});

const wallet = Keypair.fromSecretKey(bs58.decode(requireEnv("PRIVATE_KEY")));
const TARGET_WALLET = new PublicKey(requireEnv("TARGET_WALLET"));

const BUY_AMOUNT = Number(process.env.BUY_AMOUNT_SOL) * 1e9;
const BASE_PRIORITY_FEE = Number(process.env.PRIORITY_FEE_LAMPORTS ?? 500_000);
const SLIPPAGE_BPS = Number(process.env.SLIPPAGE_BPS ?? 1500);
const MIN_BUY_SOL = Number(process.env.MIN_BUY_SOL ?? 0.01) * 1e9;

const SOL_MINT = "So11111111111111111111111111111111111111112";
const JUP_BASE = "https://quote-api.jup.ag/v6";

const TP_TIERS = [
  { tp: 7.0, sellFraction: 0.5 },
  { tp: 11.0, sellFraction: 0.3 },
  { tp: 15.0, sellFraction: 0.2 },
];
const SL_PCT = -0.35;
const TRAIL_DRAWDOWN = -0.2;
const MAX_HOLD_MS = 5 * 60 * 1000;

/* ================= KNOWN SWAP PROGRAMS ================= */

const PUMPFUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const PUMPSWAP_PROGRAM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const RAYDIUM_V4_PROGRAM = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const JUPITER_PROGRAM = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
const SWAP_PROGRAMS = new Set([PUMPFUN_PROGRAM, PUMPSWAP_PROGRAM, RAYDIUM_V4_PROGRAM, JUPITER_PROGRAM]);

/* ================= PUMP.FUN 0ms EVENT DECODER ================= */

const PUMPFUN_PROGRAM_ID = new PublicKey(PUMPFUN_PROGRAM);
let pumpEventCoder: BorshEventCoder | null = null;

async function initPumpDecoder() {
  // Mock provider for IDL fetching (read-only)
  const provider = new AnchorProvider(connection, {} as any, {});
  try {
    const idl = await Program.fetchIdl(PUMPFUN_PROGRAM_ID, provider);
    if (!idl) {
      console.log("⚠️ Could not fetch Pump.fun IDL — falling back to getTransaction path only.");
      return;
    }
    pumpEventCoder = new BorshEventCoder(idl as any);
    console.log("✅ 0ms Pump.fun Event Decoder Online");
  } catch (e: any) {
    console.log("⚠️ Failed to initialize Pump IDL:", e.message);
  }
}

function tryFastDecode(logs: string[]): { mint: string; isBuy: boolean; solAmount: number; user: string } | null {
  if (!pumpEventCoder) return null;
  for (const log of logs) {
    if (!log.startsWith("Program data: ")) continue;
    try {
      const decoded = pumpEventCoder.decode(log.slice("Program data: ".length));
      if (decoded?.name === "TradeEvent") {
        const d: any = decoded.data;
        return {
          mint: d.mint?.toString(),
          isBuy: d.isBuy,
          solAmount: Number(d.solAmount),
          user: d.user?.toString(),
        };
      }
    } catch {
      // Not a TradeEvent log, ignore and continue
    }
  }
  return null;
}

/* ================= TRANSACTION PARSING (FALLBACK) ================= */

function getResolvedAccountKeys(tx: any): PublicKey[] {
  const staticKeys: PublicKey[] = tx.transaction?.message?.staticAccountKeys ?? [];
  const loaded = tx.meta?.loadedAddresses;
  const writable: PublicKey[] = loaded?.writable ?? [];
  const readonly: PublicKey[] = loaded?.readonly ?? [];
  return [...staticKeys, ...writable, ...readonly];
}

function isRealSwap(tx: any): boolean {
  const keys = getResolvedAccountKeys(tx).map((k) => k.toString());
  return keys.some((key) => SWAP_PROGRAMS.has(key));
}

function getTargetSolDelta(tx: any): number | null {
  const keys = getResolvedAccountKeys(tx);
  const idx = keys.findIndex((k) => k.toString() === TARGET_WALLET.toString());
  if (idx === -1 || !tx.meta?.preBalances || !tx.meta?.postBalances) return null;
  return tx.meta.preBalances[idx] - tx.meta.postBalances[idx];
}

/* ================= JITO ================= */

const JITO_BLOCK_ENGINE_URL = "https://mainnet.block-engine.jito.wtf/api/v1/bundles";
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
const JITO_TIP_LAMPORTS = Number(process.env.JITO_TIP_LAMPORTS ?? 2_000_000);

function randomTipAccount(): PublicKey {
  const idx = Math.floor(Math.random() * JITO_TIP_ACCOUNTS.length);
  const addr = JITO_TIP_ACCOUNTS[idx] ?? JITO_TIP_ACCOUNTS[0];
  return new PublicKey(addr as string);
}

/* ================= NETWORK ================= */

const agent = new https.Agent({ keepAlive: true });

async function fetchJson(url: string, options: any = {}) {
  return fetch(url, { agent, ...options }).then((r: any) => r.json());
}

function getPriorityFee() {
  return Math.floor(BASE_PRIORITY_FEE * (1 + Math.random()));
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
}

const positions = new Map<string, Position>();
const inFlight = new Set<string>();
const seenSignatures = new Set<string>();

/* ================= JUPITER ================= */

async function getQuote(inputMint: string, outputMint: string, amount: number) {
  const url = `${JUP_BASE}/quote?inputMint=${inputMint}&outputMint=${outputMint}&amount=${Math.floor(
    amount
  )}&slippageBps=${SLIPPAGE_BPS}`;
  const data = await fetchJson(url);
  if (!data || data.error || !data.outAmount) return null;
  return data;
}

async function buildSwapTx(quoteResponse: any) {
  const data = await fetchJson(`${JUP_BASE}/swap`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      quoteResponse,
      userPublicKey: wallet.publicKey.toString(),
      wrapAndUnwrapSol: true,
      prioritizationFeeLamports: getPriorityFee(),
      dynamicComputeUnitLimit: true,
    }),
  });
  if (!data || data.error || !data.swapTransaction) {
    console.log("Swap build error:", data?.error ?? "unknown");
    return null;
  }
  return data.swapTransaction;
}

async function sendSwapViaJito(txBase64: string): Promise<string | null> {
  try {
    const swapTx = VersionedTransaction.deserialize(Buffer.from(txBase64, "base64"));
    swapTx.sign([wallet]);

    const { blockhash } = await connection.getLatestBlockhash("confirmed");
    const tipTx = new Transaction({ feePayer: wallet.publicKey, recentBlockhash: blockhash }).add(
      SystemProgram.transfer({
        fromPubkey: wallet.publicKey,
        toPubkey: randomTipAccount(),
        lamports: JITO_TIP_LAMPORTS,
      })
    );
    tipTx.sign(wallet);

    const bundle = [bs58.encode(swapTx.serialize()), bs58.encode(tipTx.serialize())];
    const res = await fetchJson(JITO_BLOCK_ENGINE_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "sendBundle", params: [bundle] }),
    });

    if (!res?.result) {
      console.log("Jito bundle rejected:", res?.error ?? "unknown");
      return null;
    }
    console.log("📦 Bundle submitted:", res.result);
    return res.result;
  } catch (e: any) {
    console.log("Jito send error:", e.message);
    return null;
  }
}

/* ================= BALANCE ================= */

async function getTokenBalance(mint: string): Promise<number> {
  try {
    const accs = await connection.getParsedTokenAccountsByOwner(wallet.publicKey, {
      mint: new PublicKey(mint),
    });
    return Number(accs.value[0]?.account?.data?.parsed?.info?.tokenAmount?.amount ?? 0);
  } catch {
    return 0;
  }
}

async function waitForBalance(mint: string, attempts = 6, delayMs = 500): Promise<number> {
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
  if (impact > 0.15) {
    console.log("❌ Skipped — price impact too high:", impact);
    return false;
  }
  return Number(quote.outAmount) > 0;
}

/* ================= BUY ================= */

async function executeBuy(mint: string) {
  if (positions.has(mint) || inFlight.has(mint)) return;
  inFlight.add(mint);

  try {
    const quote = await getQuote(SOL_MINT, mint, BUY_AMOUNT);
    if (!quote || !passesFilters(quote)) return;

    const tx = await buildSwapTx(quote);
    if (!tx) return;

    const sig = await sendSwapViaJito(tx);
    if (!sig) return;

    console.log("🚀 BUY bundle sent:", mint, sig);

    const actual = await waitForBalance(mint);
    if (actual <= 0) {
      console.log("⚠️ Could not confirm balance for", mint, "— position NOT tracked. Check manually.");
      return;
    }

    positions.set(mint, {
      mint,
      originalAmount: actual,
      remainingAmount: actual,
      costBasisLamports: BUY_AMOUNT,
      entryTime: Date.now(),
      highestValue: 0,
      tiersHit: TP_TIERS.map(() => false),
    });
  } finally {
    inFlight.delete(mint);
  }
}

/* ================= SELL ================= */

async function executeSell(mint: string, amount: number, reason: string) {
  const pos = positions.get(mint);
  if (!pos) return;

  const quote = await getQuote(mint, SOL_MINT, amount);
  if (!quote) return;

  const tx = await buildSwapTx(quote);
  if (!tx) return;

  const sig = await sendSwapViaJito(tx);
  if (!sig) return;

  console.log(`✅ SELL (${reason}):`, mint, sig);

  const soldFraction = amount / pos.remainingAmount;
  pos.remainingAmount -= amount;
  pos.costBasisLamports -= pos.costBasisLamports * soldFraction;

  if (pos.remainingAmount <= 0) positions.delete(mint);
}

/* ================= MONITOR ================= */

async function monitor() {
  for (const [mint, pos] of positions.entries()) {
    if (pos.remainingAmount <= 0) continue;

    getQuote(mint, SOL_MINT, pos.remainingAmount).then((quote) => {
      if (!quote) return;

      const value = Number(quote.outAmount);
      const pnl = (value - pos.costBasisLamports) / pos.costBasisLamports;

      if (pnl <= SL_PCT) {
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

/* ================= DETECTION (FAST PATH + FALLBACK) ================= */

async function handleTx(signature: string) {
  if (seenSignatures.has(signature)) return;
  seenSignatures.add(signature);

  try {
    const tx = await connection.getTransaction(signature, {
      commitment: "confirmed",
      maxSupportedTransactionVersion: 0,
    });
    if (!tx?.meta) return;

    if (!isRealSwap(tx)) return;

    const solDelta = getTargetSolDelta(tx);
    if (solDelta !== null && solDelta < MIN_BUY_SOL) {
      console.log("⏭️ Skipped — below SOL threshold:", solDelta / 1e9);
      return;
    }

    for (const b of tx.meta.postTokenBalances ?? []) {
      if (b.owner !== TARGET_WALLET.toString()) continue;
      if (b.mint === SOL_MINT) continue;

      console.log("🔥 DETECTED real swap buy (Fallback Path):", b.mint);
      executeBuy(b.mint);
    }
  } catch (e: any) {
    console.log("Detection error:", e.message);
  }
}

/* ================= STARTUP LOGIC ================= */

const app = express();
app.get("/", (_req, res) => res.send("Engine Active"));
const PORT = process.env.PORT || 3000;

async function startEngine() {
  console.log("🚀 Initializing Copy-Trade V11 Engine...");
  
  // 1. Boot up the 0ms Pump.fun IDL decoder first
  await initPumpDecoder();

  // 2. Start the Keep-Alive Web Server
  app.listen(PORT, () => console.log(`🌐 Keep-alive server on port ${PORT}`));
  console.log("🎯 Target Locked:", TARGET_WALLET.toString());

  // 3. Attach the hyper-fast WebSocket listener
  connection.onLogs(
    TARGET_WALLET,
    (log) => {
      // THE FAST PATH: Instant Borsh Decoding
      const fast = tryFastDecode(log.logs);
      if (fast && fast.user === TARGET_WALLET.toString() && fast.isBuy && fast.solAmount >= MIN_BUY_SOL) {
        console.log("⚡ FAST-PATH DETECTED BUY:", fast.mint);
        executeBuy(fast.mint);
        return; // Skip the slower fallback path entirely
      }
      
      // THE FALLBACK PATH: If it's not a Pump.fun trade (e.g., Raydium), or IDL failed
      handleTx(log.signature);
    },
    "processed"
  );
}

// Ignite the engine
startEngine();