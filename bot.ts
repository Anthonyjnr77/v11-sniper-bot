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
const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS ?? 5000);

const SOL_MINT = "So11111111111111111111111111111111111111112";

const JUP_BASE = "https://api.jup.ag/swap/v1";
const JUP_API_KEY = process.env.JUP_API_KEY;

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

/* ================= CRASH ALERTS ================= */

process.on("uncaughtException", async (err) => {
  console.log("💥 UNCAUGHT EXCEPTION:", err.message);
  await sendTelegramAlert(`💥 <b>BOT CRASHED</b>\n${err.message}\nRender should auto-restart it.`);
});

process.on("unhandledRejection", async (reason: any) => {
  console.log("💥 UNHANDLED REJECTION:", reason);
  await sendTelegramAlert(`💥 <b>BOT ERROR</b>\n${String(reason).slice(0, 200)}`);
});

/* ================= PUMP.FUN FAST-PATH DECODER ================= */

const PUMPFUN_PROGRAM = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";

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
  return Math.floor(BASE_PRIORITY_FEE * (1 + Math.random()));
}

function jupHeaders(extra: Record<string, string> = {}) {
  return JUP_API_KEY ? { "x-api-key": JUP_API_KEY, ...extra } : extra;
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
  return mc >= 1000 ? `$${(mc / 1000).toFixed(1)}K` : `$${mc.toFixed(0)}`;
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
  const data = await fetchJson(url, { headers: jupHeaders() });
  if (!data || data.error || !data.outAmount) return null;
  return data;
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

async function executeBuy(
  mint: string,
  targetSnapshotPromise?: Promise<{ tokenPriceUSD: number | null; marketCapUSD: number | null }>
) {
  if (positions.has(mint) || inFlight.has(mint)) return;
  inFlight.add(mint);

  try {
    const quote = await getQuote(SOL_MINT, mint, BUY_AMOUNT);
    if (!quote || !passesFilters(quote)) {
      console.log("❌ Buy aborted — bad/failed quote for", mint);
      return;
    }

    const tx = await buildSwapTx(quote);
    if (!tx) {
      console.log("❌ Buy aborted — swap build failed for", mint);
      return;
    }

    const sig = await sendSwapDual(tx);
    if (!sig) {
      console.log("❌ Buy aborted — both Jito and RPC submission failed for", mint);
      return;
    }

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

    const [mySnapshot, targetSnapshot] = await Promise.all([
      fetchPriceAndMarketCap(mint),
      targetSnapshotPromise ?? Promise.resolve({ tokenPriceUSD: null, marketCapUSD: null }),
    ]);

    const cleanSig = sig.split(":")[1] || sig;
    await sendTelegramAlert(
      `🚀 <b>BUY EXECUTED</b>\n` +
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

  const { marketCapUSD } = await fetchPriceAndMarketCap(mint);

  const cleanSig = sig.split(":")[1] || sig;
  await sendTelegramAlert(
    `✅ <b>SELL EXECUTED</b> (${reason})\n` +
    `Mint: <code>${mint}</code>\n` +
    `Market Cap: ${fmtMC(marketCapUSD)}\n` +
    `<a href="https://solscan.io/tx/${cleanSig}">View on Solscan</a>`
  );
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

/* ================= DETECTION (Router-Agnostic fallback, with retry + full logging) ================= */

function getResolvedAccountKeys(tx: any): PublicKey[] {
  const staticKeys: PublicKey[] = tx.transaction?.message?.staticAccountKeys ?? [];
  const loaded = tx.meta?.loadedAddresses;
  const writable: PublicKey[] = loaded?.writable ?? [];
  const readonly: PublicKey[] = loaded?.readonly ?? [];
  return [...staticKeys, ...writable, ...readonly];
}

async function getTransactionWithRetry(signature: string, attempts = 6, delayMs = 400) {
  for (let i = 0; i < attempts; i++) {
    const tx = await connection.getTransaction(signature, {
      commitment: "confirmed",
      maxSupportedTransactionVersion: 0,
    });
    if (tx?.meta) return tx;
    await new Promise((r) => setTimeout(r, delayMs));
  }
  return null;
}

async function handleTx(signature: string) {
  if (seenSignatures.has(signature)) {
    console.log("↩️ Duplicate signature, skipping:", signature);
    return;
  }
  seenSignatures.add(signature);

  try {
    const tx = await getTransactionWithRetry(signature);
    if (!tx?.meta) {
      console.log("❌ Still no tx/meta after retries for", signature, "— genuine miss");
      return;
    }

    const keys = getResolvedAccountKeys(tx);
    const targetIdx = keys.findIndex((k) => k.toString() === TARGET_WALLET.toString());
    if (targetIdx === -1) {
      console.log("❌ Target wallet not found in account keys for", signature);
      return;
    }

    const solDelta = (tx.meta.preBalances?.[targetIdx] ?? 0) - (tx.meta.postBalances?.[targetIdx] ?? 0);
    if (solDelta < MIN_BUY_SOL) {
      console.log(`⏭️ Skipped — SOL delta ${(solDelta / 1e9).toFixed(4)} below threshold`, signature);
      return;
    }

    const preBalances = tx.meta.preTokenBalances ?? [];
    const postBalances = tx.meta.postTokenBalances ?? [];

    let foundAnyIncrease = false;
    for (const post of postBalances) {
      if (post.owner !== TARGET_WALLET.toString()) continue;
      if (post.mint === SOL_MINT) continue;

      const pre = preBalances.find((p) => p.owner === post.owner && p.mint === post.mint);
      const preAmount = pre ? Number(pre.uiTokenAmount.uiAmount) : 0;
      const postAmount = Number(post.uiTokenAmount.uiAmount);

      if (postAmount > preAmount) {
        foundAnyIncrease = true;
        console.log("🔥 DETECTED router-agnostic buy:", post.mint, signature);
        const targetSnapshotPromise = fetchPriceAndMarketCap(post.mint);
        executeBuy(post.mint, targetSnapshotPromise);
      }
    }

    if (!foundAnyIncrease) {
      console.log("❌ SOL decreased but no token balance increase found for target wallet —", signature);
    }
  } catch (e: any) {
    console.log("Detection error:", e.message, signature);
  }
}

/* ================= POLLING RECONCILIATION LOOP (correctness backstop) ================= */
// This is the actual fix for silent websocket misses. onLogs is best-effort with
// no delivery guarantee by design — this queries the committed ledger directly,
// so it cannot drop an event the way a websocket stream can. The websocket stays
// as the fast path; this guarantees nothing is ever permanently missed.

let lastPolledSignature: string | null = null;

async function initPollingCursor() {
  try {
    const sigs = await connection.getSignaturesForAddress(TARGET_WALLET, { limit: 1 }, "confirmed");
    if (sigs.length > 0) {
      lastPolledSignature = sigs[0].signature;
      console.log("✅ Polling cursor initialized at:", lastPolledSignature);
    }
  } catch (e: any) {
    console.log("⚠️ Could not initialize polling cursor:", e.message);
  }
}

async function pollForMissedTrades() {
  try {
    const options: any = { limit: 25 };
    if (lastPolledSignature) options.until = lastPolledSignature;

    const sigs = await connection.getSignaturesForAddress(TARGET_WALLET, options, "confirmed");
    if (sigs.length === 0) return;

    // sigs come back newest-first; process oldest-first for correct chronological order
    const ordered = [...sigs].reverse();

    for (const sigInfo of ordered) {
      if (sigInfo.err) continue; // skip failed on-chain transactions
      if (!seenSignatures.has(sigInfo.signature)) {
        console.log("🔎 POLL found unprocessed signature (websocket likely missed it):", sigInfo.signature);
        await handleTx(sigInfo.signature);
      }
    }

    lastPolledSignature = sigs[0].signature;
  } catch (e: any) {
    console.log("Polling error:", e.message);
  }
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
  await initPollingCursor();

  let subId: number | null = null;

  function subscribeToWallet() {
    const oldSubId = subId;

    console.log("🔌 Opening new WebSocket subscription...");
    subId = connection.onLogs(
      TARGET_WALLET,
      (log) => {
        console.log(`👀 Raw activity heard: https://solscan.io/tx/${log.signature}`);

        const fast = tryFastDecode(log.logs);
        if (fast) {
          if (fast.user !== TARGET_WALLET.toString()) {
            console.log("⏭️ Fast-path: decoded event but user mismatch — ignoring, no fallback");
            return;
          }
          if (!fast.isBuy) {
            console.log("⏭️ Fast-path: confirmed SELL by target wallet — ignoring, no fallback");
            return;
          }
          if (fast.solAmount < MIN_BUY_SOL) {
            console.log(`⏭️ Fast-path: solAmount ${(fast.solAmount / 1e9).toFixed(4)} below threshold — ignoring, no fallback`);
            return;
          }
          console.log("⚡ FAST-PATH detected buy:", fast.mint);
          const targetSnapshotPromise = fetchPriceAndMarketCap(fast.mint);
          executeBuy(fast.mint, targetSnapshotPromise);
          return;
        }

        console.log("↪️ Fast-path found no TradeEvent — falling back to getTransaction");
        handleTx(log.signature);
      },
      "processed"
    );
    console.log("✅ New subscription active:", subId);

    if (oldSubId !== null) {
      setTimeout(() => {
        connection.removeOnLogsListener(oldSubId).catch(() => {});
        console.log("🗑️ Old subscription removed:", oldSubId);
      }, 3000);
    }
  }

  subscribeToWallet();
  setInterval(subscribeToWallet, 60 * 1000);
  setInterval(pollForMissedTrades, POLL_INTERVAL_MS);

  console.log("✅ Bot fully running (websocket + polling backstop active)");
  await sendTelegramAlert(`🟢 <b>V11 Engine Online</b>\nSniper deployed and actively watching target wallet:\n<code>${TARGET_WALLET.toString()}</code>`);
}

start();