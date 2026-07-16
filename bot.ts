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
  commitment: "processed",
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

/* ================= TELEGRAM ALERTS ================= */

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

async function sendTelegramAlert(message: string) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;
  const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`;
  try {
    await fetch(url, {
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

/* ================= KNOWN SWAP PROGRAMS ================= */

const PUMPFUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const PUMPSWAP_PROGRAM = "pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA";
const RAYDIUM_V4_PROGRAM = "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8";
const JUPITER_PROGRAM = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";
const SWAP_PROGRAMS = new Set([PUMPFUN_PROGRAM, PUMPSWAP_PROGRAM, RAYDIUM_V4_PROGRAM, JUPITER_PROGRAM]);

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

/* ================= PUMP.FUN FAST-PATH DECODER ================= */

let pumpEventCoder: BorshEventCoder | null = null;

async function initPumpDecoder() {
  try {
    const provider = new AnchorProvider(connection, {} as any, {});
    const idl = await Program.fetchIdl(new PublicKey(PUMPFUN_PROGRAM), provider);
    if (!idl) {
      console.log("⚠️ Could not fetch Pump.fun IDL — fast path disabled.");
      return;
    }
    pumpEventCoder = new BorshEventCoder(idl as any);
    console.log("✅ Pump.fun fast-path decoder ready");
  } catch (e: any) {
    console.log("⚠️ Pump.fun decoder init failed:", e.message);
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
        const mint = d.mint?.toString();
        const user = d.user?.toString();
        if (!mint || !user) return null;
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

/* ================= PERSISTENCE (Upstash Redis) ================= */

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

async function persistPositions() {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) return;
  try {
    const serialized = JSON.stringify(Array.from(positions.entries()));
    await fetch(`${UPSTASH_URL}/set/positions`, {
      method: "POST",
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
      body: serialized,
    });
  } catch (e: any) {
    console.log("Persist error:", e.message);
  }
}

async function loadPositions() {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) return;
  try {
    const res = await fetch(`${UPSTASH_URL}/get/positions`, {
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
    });
    const data: any = await res.json();
    if (data?.result) {
      const entries = JSON.parse(data.result);
      for (const [mint, pos] of entries) positions.set(mint, pos);
      console.log(`✅ Restored ${entries.length} position(s) from before restart`);
    }
  } catch (e: any) {
    console.log("Load error:", e.message);
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
  if (!data || data.error || !data.swapTransaction) return null;
  return data.swapTransaction;
}

/* ================= DUAL-PATH SUBMISSION ================= */

async function sendSwapDual(txBase64: string): Promise<string | null> {
  const swapTx = VersionedTransaction.deserialize(Buffer.from(txBase64, "base64"));
  swapTx.sign([wallet]);
  const rawBytes = swapTx.serialize();

  const jitoAttempt = (async () => {
    try {
      const { blockhash } = await connection.getLatestBlockhash("processed");
      const tipTx = new Transaction({ feePayer: wallet.publicKey, recentBlockhash: blockhash }).add(
        SystemProgram.transfer({
          fromPubkey: wallet.publicKey,
          toPubkey: randomTipAccount(),
          lamports: JITO_TIP_LAMPORTS,
        })
      );
      tipTx.sign(wallet);

      const bundle = [bs58.encode(rawBytes), bs58.encode(tipTx.serialize())];
      const res = await fetchJson(JITO_BLOCK_ENGINE_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "sendBundle", params: [bundle] }),
      });
      return res?.result ? `jito:${res.result}` : null;
    } catch {
      return null;
    }
  })();

  const rpcAttempt = (async () => {
    try {
      const sig = await connection.sendRawTransaction(rawBytes, { skipPreflight: true, maxRetries: 3 });
      return `rpc:${sig}`;
    } catch {
      return null;
    }
  })();

  const results = await Promise.allSettled([jitoAttempt, rpcAttempt]);
  for (const r of results) {
    if (r.status === "fulfilled" && r.value) {
      console.log("✅ Landed via:", r.value);
      return r.value;
    }
  }
  return null;
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
  return impact <= 0.15 && Number(quote.outAmount) > 0;
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

    const sig = await sendSwapDual(tx);
    if (!sig) return;

    console.log("🚀 BUY sent:", mint, sig);

    const actual = await waitForBalance(mint);
    if (actual <= 0) {
      console.log("⚠️ Could not confirm balance for", mint);
      await sendTelegramAlert(`⚠️ <b>BALANCE WARNING</b>\nCould not confirm balance for <code>${mint}</code> after buy. Check manually.`);
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
    
    await persistPositions();
    
    // Telegram Buy Alert
    const cleanSig = sig.split(":")[1] || sig;
    await sendTelegramAlert(`🚀 <b>BUY EXECUTED</b>\nMint: <code>${mint}</code>\nAmount: ${(BUY_AMOUNT / 1e9).toFixed(3)} SOL\n<a href="https://solscan.io/tx/${cleanSig}">View on Solscan</a>`);
    
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

  const sig = await sendSwapDual(tx);
  if (!sig) return;

  console.log(`✅ SELL (${reason}):`, mint, sig);

  const soldFraction = amount / pos.remainingAmount;
  pos.remainingAmount -= amount;
  pos.costBasisLamports -= pos.costBasisLamports * soldFraction;

  if (pos.remainingAmount <= 0) positions.delete(mint);
  await persistPositions();

  // Telegram Sell Alert
  const cleanSig = sig.split(":")[1] || sig;
  await sendTelegramAlert(`✅ <b>SELL EXECUTED</b> (${reason})\nMint: <code>${mint}</code>\n<a href="https://solscan.io/tx/${cleanSig}">View on Solscan</a>`);
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

/* ================= DETECTION ================= */

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
    if (solDelta !== null && solDelta < MIN_BUY_SOL) return;

    for (const b of tx.meta.postTokenBalances ?? []) {
      if (b.owner !== TARGET_WALLET.toString()) continue;
      if (b.mint === SOL_MINT) continue;

      executeBuy(b.mint);
    }
  } catch (e: any) {}
}

/* ================= KEEP-ALIVE SERVER ================= */

const app = express();
app.get("/", (_req, res) => res.send("Engine Active"));
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Keep-alive server on port ${PORT}`));

/* ================= START ================= */

async function start() {
  console.log("🚀 Copy-trade bot starting. Target:", TARGET_WALLET.toString());
  await loadPositions();
  await initPumpDecoder();

  let subId: number = 0;

  function subscribeToWallet() {
    // Cleanly remove any existing listener to prevent duplicate events or memory leaks
    if (subId) {
      connection.removeOnLogsListener(subId).catch(() => {});
    }
    
    console.log("🔌 (Re)connecting WebSocket to listen for trades...");
    
    subId = connection.onLogs(
      TARGET_WALLET,
      (log) => {
        // THE LIE DETECTOR: This will print every single time the wallet breathes.
        console.log(`👀 Raw activity heard: https://solscan.io/tx/${log.signature}`);
        
        const fast = tryFastDecode(log.logs);
        if (fast && fast.user === TARGET_WALLET.toString() && fast.isBuy && fast.solAmount >= MIN_BUY_SOL) {
          console.log("⚡ FAST-PATH detected buy:", fast.mint);
          executeBuy(fast.mint);
          return;
        }
        
        // Fallback path
        handleTx(log.signature);
      },
      "processed"
    );
  }

  // Start the first connection
  subscribeToWallet();

  // Force-refresh the WebSocket every 5 minutes (300,000 ms) to prevent silent drops
  setInterval(subscribeToWallet, 5 * 60 * 1000);

  console.log("✅ Bot fully running");
  await sendTelegramAlert(`🟢 <b>V11 Engine Online</b>\nSniper deployed and actively watching target wallet:\n<code>${TARGET_WALLET.toString()}</code>`);
}

start();