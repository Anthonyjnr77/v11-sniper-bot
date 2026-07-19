import "dotenv/config";
import {
  Connection,
  Keypair,
  VersionedTransaction,
  PublicKey,
  SystemProgram,
  Transaction,
  TransactionInstruction,
  ComputeBudgetProgram,
} from "@solana/web3.js";
import type { Logs } from "@solana/web3.js";
import { Program, AnchorProvider, BorshEventCoder } from "@coral-xyz/anchor";
import { getAssociatedTokenAddress } from "@solana/spl-token";
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

const agent = new https.Agent({ keepAlive: true, maxSockets: 64 });

function makeConnection(url: string): Connection {
  return new Connection(url, {
    commitment: "processed",
    wsEndpoint: url.replace("https", "wss"),
    httpAgent: agent,
  });
}

const connection = makeConnection(RPC_URL);

const WS_FANOUT = Math.max(1, Number(process.env.WS_FANOUT ?? 2));
const EXTRA_WS_URLS = (process.env.EXTRA_WS_URLS ?? "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const detectionConnections: Connection[] = [connection];
for (let i = 1; i < WS_FANOUT; i++) detectionConnections.push(makeConnection(RPC_URL));
for (const url of EXTRA_WS_URLS) detectionConnections.push(makeConnection(url));

const BROADCAST_RPC_URL = process.env.BROADCAST_RPC_URL ?? RPC_URL;
const broadcastConnection = makeConnection(BROADCAST_RPC_URL);

const wallet = Keypair.fromSecretKey(bs58.decode(requireEnv("PRIVATE_KEY")));
const TARGET_WALLET = new PublicKey(requireEnv("TARGET_WALLET"));

const BUY_AMOUNT_SOL = Number(process.env.BUY_AMOUNT_SOL);
const BUY_AMOUNT = BUY_AMOUNT_SOL * 1e9;
const BASE_PRIORITY_FEE = Number(process.env.PRIORITY_FEE_LAMPORTS ?? 500_000);
const SLIPPAGE_BPS = Number(process.env.SLIPPAGE_BPS ?? 1500);
const MIN_BUY_SOL = Number(process.env.MIN_BUY_SOL ?? 0.01) * 1e9;
const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS ?? 5000);
const MAX_PRICE_IMPACT = Number(process.env.MAX_PRICE_IMPACT ?? 0.15);
const ONLY_DIRECT_ROUTES = (process.env.ONLY_DIRECT_ROUTES ?? "true") === "true";
const PUMPPORTAL_ENABLED = (process.env.PUMPPORTAL ?? "true") === "true";
const MIN_WALLET_BALANCE_SOL = Number(process.env.MIN_WALLET_BALANCE_SOL ?? 0.05);

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

// Pump.fun constants
const PUMPFUN_PROGRAM = new PublicKey("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P");
const PUMP_GLOBAL = new PublicKey("4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf");
const PUMP_EVENT_AUTHORITY = new PublicKey("Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1");
const PUMP_FEE_RECIPIENT = new PublicKey("CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicJhtHTAcVUN");
const TOKEN_PROGRAM = new PublicKey("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
const ASSOCIATED_TOKEN_PROGRAM = new PublicKey("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL");

/* ================= TELEGRAM ================= */

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

async function sendTelegramAlert(message: string) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;
  try {
    await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
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
    console.log("Telegram failed:", e.message);
  }
}

/* ================= CRASH ALERTS ================= */

process.on("uncaughtException", async (err) => {
  console.log("💥 UNCAUGHT EXCEPTION:", err.message);
  await sendTelegramAlert(`💥 <b>BOT CRASHED</b>\n${err.message}`).catch(() => {});
});

process.on("unhandledRejection", async (reason: any) => {
  console.log("💥 UNHANDLED REJECTION:", reason);
  await sendTelegramAlert(`💥 <b>BOT ERROR</b>\n${String(reason).slice(0, 200)}`).catch(() => {});
});

/* ================= PUMP.FUN DECODER ================= */

let pumpEventCoder: BorshEventCoder | null = null;

async function initPumpDecoder() {
  try {
    const provider = new AnchorProvider(connection, {} as any, {});
    const idl = await Program.fetchIdl(PUMPFUN_PROGRAM, provider);
    if (!idl) {
      console.log("⚠️ Could not fetch Pump.fun IDL");
      return;
    }
    pumpEventCoder = new BorshEventCoder(idl as any);
    console.log("✅ Pump.fun decoder ready");
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
        return { mint, isBuy: Boolean(d.isBuy), solAmount: Number(d.solAmount ?? 0), user };
      }
    } catch {}
  }
  return null;
}

/* ================= JITO ================= */

const JITO_BLOCK_ENGINE_URL = process.env.JITO_URL ?? "https://mainnet.block-engine.jito.wtf/api/v1/bundles";
const JITO_TIP_ACCOUNTS = [
  "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
  "HFqU5x63VTqvQss8hp11i4e3Kk5aQfvAfEMn7hPfqLfE",
  "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
  "ADaUMid9yfUytqMBgopwjb2DTLsD9A2CboQdKQACQgkP",
];
const JITO_TIP_LAMPORTS = Number(process.env.JITO_TIP_LAMPORTS ?? 2_000_000);

function randomTipAccount(): PublicKey {
  return new PublicKey(JITO_TIP_ACCOUNTS[Math.floor(Math.random() * JITO_TIP_ACCOUNTS.length)]!);
}

/* ================= BLOCK DATA CACHE ================= */

let cachedBlockhash = "";
let cachedPriorityFee = BASE_PRIORITY_FEE;

async function refreshBlockData() {
  try {
    const [blockhash, fees] = await Promise.all([
      connection.getLatestBlockhash("processed").then(b => b.blockhash),
      connection.getRecentPrioritizationFees().catch(() => [] as any[]),
    ]);
    cachedBlockhash = blockhash;
    if (fees.length > 0) {
      const sorted = fees.sort((a: any, b: any) => a.prioritizationFee - b.prioritizationFee);
      const median = sorted[Math.floor(sorted.length / 2)]!.prioritizationFee;
      cachedPriorityFee = Math.max(BASE_PRIORITY_FEE, Math.floor(median * 1.2));
    }
  } catch {}
}

/* ================= FETCH HELPERS ================= */

function jupHeaders(extra: Record<string, string> = {}) {
  return JUP_API_KEY ? { "x-api-key": JUP_API_KEY, ...extra } : extra;
}

async function fetchJson(url: string, options: any = {}, retries = 2): Promise<any> {
  for (let i = 0; i <= retries; i++) {
    try {
      const res = await fetch(url, { ...options, agent });
      return await res.json();
    } catch (e: any) {
      if (i === retries) throw e;
      await new Promise(r => setTimeout(r, 300));
    }
  }
}

async function getQuote(inputMint: string, outputMint: string, amount: number) {
  let url = `${JUP_BASE}/quote?inputMint=${inputMint}&outputMint=${outputMint}&amount=${Math.floor(amount)}&slippageBps=${SLIPPAGE_BPS}`;
  if (ONLY_DIRECT_ROUTES) url += "&onlyDirectRoutes=true";
  const data = await fetchJson(url, { headers: jupHeaders() });
  return data?.outAmount ? data : null;
}

async function buildSwapTx(quoteResponse: any): Promise<VersionedTransaction | null> {
  const data = await fetchJson(`${JUP_BASE}/swap`, {
    method: "POST",
    headers: jupHeaders({ "Content-Type": "application/json" }),
    body: JSON.stringify({
      quoteResponse,
      userPublicKey: wallet.publicKey.toString(),
      wrapAndUnwrapSol: true,
      prioritizationFeeLamports: cachedPriorityFee,
      dynamicComputeUnitLimit: true,
      dynamicSlippage: true,
    }),
  });
  if (!data?.swapTransaction) return null;
  return VersionedTransaction.deserialize(Buffer.from(data.swapTransaction, "base64"));
}

/* ================= DIRECT PUMP.FUN BUY (60-200ms) ================= */

async function buildPumpFunBuyIx(mint: PublicKey, solAmount: number): Promise<TransactionInstruction> {
  const [bondingCurve] = PublicKey.findProgramAddressSync(
    [Buffer.from("bonding-curve"), mint.toBuffer()],
    PUMPFUN_PROGRAM
  );

  const [bondingCurveTokenAccount] = PublicKey.findProgramAddressSync(
    [bondingCurve.toBuffer(), TOKEN_PROGRAM.toBuffer(), mint.toBuffer()],
    ASSOCIATED_TOKEN_PROGRAM
  );

  const userTokenAccount = await getAssociatedTokenAddress(mint, wallet.publicKey);

  const discriminator = Buffer.alloc(8);
  discriminator.writeBigUInt64LE(BigInt("16927863322537952870"), 0);

  const amountBuffer = Buffer.alloc(8);
  amountBuffer.writeBigUInt64LE(BigInt(solAmount), 0);

  const maxSolCostBuffer = Buffer.alloc(8);
  maxSolCostBuffer.writeBigUInt64LE(BigInt(0), 0);

  return new TransactionInstruction({
    programId: PUMPFUN_PROGRAM,
    keys: [
      { pubkey: PUMP_GLOBAL, isSigner: false, isWritable: false },
      { pubkey: PUMP_FEE_RECIPIENT, isSigner: false, isWritable: true },
      { pubkey: mint, isSigner: false, isWritable: false },
      { pubkey: bondingCurve, isSigner: false, isWritable: true },
      { pubkey: bondingCurveTokenAccount, isSigner: false, isWritable: true },
      { pubkey: userTokenAccount, isSigner: false, isWritable: true },
      { pubkey: wallet.publicKey, isSigner: true, isWritable: true },
      { pubkey: SystemProgram.programId, isSigner: false, isWritable: false },
      { pubkey: TOKEN_PROGRAM, isSigner: false, isWritable: false },
      { pubkey: PUMP_EVENT_AUTHORITY, isSigner: false, isWritable: false },
      { pubkey: PUMPFUN_PROGRAM, isSigner: false, isWritable: false },
    ],
    data: Buffer.concat([discriminator, amountBuffer, maxSolCostBuffer]),
  });
}

async function sendJitoBundle(rawTx: Uint8Array): Promise<string | null> {
  try {
    const tipTx = new Transaction().add(
      SystemProgram.transfer({
        fromPubkey: wallet.publicKey,
        toPubkey: randomTipAccount(),
        lamports: JITO_TIP_LAMPORTS,
      })
    );
    tipTx.feePayer = wallet.publicKey;
    tipTx.recentBlockhash = cachedBlockhash;
    tipTx.sign(wallet);

    const bundle = [bs58.encode(rawTx), bs58.encode(tipTx.serialize())];
    const res = await fetch(JITO_BLOCK_ENGINE_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ jsonrpc: "2.0", id: 1, method: "sendBundle", params: [bundle] }),
    });
    const data: any = await res.json();
    return data?.result ? `jito:${data.result}` : null;
  } catch {
    return null;
  }
}

async function executePumpFunDirectBuy(mint: string): Promise<{ success: boolean; sig?: string; elapsedMs: number }> {
  const startTime = Date.now();

  try {
    const buyIx = await buildPumpFunBuyIx(new PublicKey(mint), BUY_AMOUNT);

    const tx = new Transaction();
    tx.feePayer = wallet.publicKey;
    tx.recentBlockhash = cachedBlockhash;
    tx.add(
      ComputeBudgetProgram.setComputeUnitLimit({ units: 200_000 }),
      ComputeBudgetProgram.setComputeUnitPrice({ microLamports: cachedPriorityFee }),
      buyIx
    );

    tx.sign(wallet);
    const rawBytes = tx.serialize();

    const jitoPromise = sendJitoBundle(rawBytes);
    const rpcPromise = connection.sendRawTransaction(rawBytes, {
      skipPreflight: true,
      maxRetries: 0,
    }).then(sig => `rpc:${sig}`).catch(() => null);

    const sig = await Promise.race([jitoPromise, rpcPromise]);
    const elapsed = Date.now() - startTime;

    if (sig) {
      console.log(`⚡ PUMP DIRECT BUY: ${mint} (${elapsed}ms)`);
      return { success: true, sig, elapsedMs: elapsed };
    }

    return { success: false, elapsedMs: elapsed };
  } catch (e: any) {
    return { success: false, elapsedMs: Date.now() - startTime };
  }
}

async function executeJupiterBuy(mint: string): Promise<boolean> {
  try {
    const quote = await getQuote(SOL_MINT, mint, BUY_AMOUNT);
    if (!quote) return false;

    const impact = Number(quote.priceImpactPct ?? 0);
    if (impact > MAX_PRICE_IMPACT) return false;

    const swapTx = await buildSwapTx(quote);
    if (!swapTx) return false;

    swapTx.sign([wallet]);
    const rawBytes = swapTx.serialize();

    const jitoPromise = sendJitoBundle(rawBytes);
    const rpcPromise = connection.sendRawTransaction(rawBytes, {
      skipPreflight: true,
      maxRetries: 0,
    }).then(sig => `rpc:${sig}`).catch(() => null);

    const sig = await Promise.race([jitoPromise, rpcPromise]);
    if (sig) {
      console.log(`🪐 JUP BUY: ${mint}`);
      return true;
    }
    return false;
  } catch (e: any) {
    console.log("Jupiter buy failed:", e.message);
    return false;
  }
}

/* ================= PRICE / MC ================= */

async function fetchPriceAndMarketCap(mint: string): Promise<{ tokenPriceUSD: number | null; marketCapUSD: number | null }> {
  try {
    const priceRes = await fetchJson(`https://api.jup.ag/price/v3?ids=${mint}`, { headers: jupHeaders() });
    const tokenPriceUSD = priceRes?.[mint]?.usdPrice ?? null;
    const supplyInfo = await connection.getTokenSupply(new PublicKey(mint));
    const supply = supplyInfo?.value?.uiAmount ?? null;
    const marketCapUSD = tokenPriceUSD && supply ? tokenPriceUSD * supply : null;
    return { tokenPriceUSD, marketCapUSD };
  } catch {
    return { tokenPriceUSD: null, marketCapUSD: null };
  }
}

function fmtMC(mc: number | null): string {
  if (mc === null) return "unknown";
  return mc >= 1000 ? `$${(mc / 1000).toFixed(1)}K` : `$${mc.toFixed(0)}`;
}

/* ================= PERSISTENCE ================= */

const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;

async function redisSet(key: string, value: string) {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) return;
  try {
    await fetch(`${UPSTASH_URL}/set/${key}`, {
      method: "POST",
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
      body: value,
    });
  } catch {}
}

async function redisGet(key: string): Promise<string | null> {
  if (!UPSTASH_URL || !UPSTASH_TOKEN) return null;
  try {
    const res = await fetch(`${UPSTASH_URL}/get/${key}`, {
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
    });
    const data: any = await res.json();
    return data?.result ?? null;
  } catch {
    return null;
  }
}

async function persistPositions() {
  await redisSet("positions", JSON.stringify(Array.from(positions.entries())));
}

async function loadPositions() {
  const raw = await redisGet("positions");
  if (raw) {
    const entries = JSON.parse(raw);
    for (const [mint, pos] of entries) positions.set(mint, pos);
    console.log(`✅ Restored ${entries.length} position(s)`);
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

async function appendTradeJournal(entry: JournalEntry) {
  try {
    const raw = await redisGet("trade_journal");
    const journal: JournalEntry[] = raw ? JSON.parse(raw) : [];
    journal.push(entry);
    while (journal.length > 500) journal.shift();
    await redisSet("trade_journal", JSON.stringify(journal));
  } catch {}
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
  entryMarketCapUSD: number | null;
}

const positions = new Map<string, Position>();
const inFlight = new Set<string>();
const seenSignatures = new Set<string>();
let botPaused = false;
let telegramUpdateOffset = 0;

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

/* ================= BUY EXECUTION ================= */

async function executeBuy(
  mint: string,
  targetSnapshotPromise?: Promise<{ tokenPriceUSD: number | null; marketCapUSD: number | null }>
) {
  if (botPaused) {
    console.log("⏸️ Buy skipped — bot paused:", mint);
    return;
  }
  if (positions.has(mint) || inFlight.has(mint)) return;
  inFlight.add(mint);

  try {
    // Try Pump.fun direct first (60-200ms)
    const pumpResult = await executePumpFunDirectBuy(mint);
    let buySig = pumpResult.sig;

    if (!pumpResult.success) {
      // Fallback to Jupiter (400-800ms)
      console.log(`⚠️ Falling back to Jupiter for ${mint}`);
      const jupSuccess = await executeJupiterBuy(mint);
      if (!jupSuccess) {
        console.log("❌ Buy failed — both paths unsuccessful");
        return;
      }
    }

    const [mySnapshot, targetSnapshot] = await Promise.all([
      fetchPriceAndMarketCap(mint),
      targetSnapshotPromise ?? Promise.resolve({ tokenPriceUSD: null, marketCapUSD: null }),
    ]);

    positions.set(mint, {
      mint,
      originalAmount: BUY_AMOUNT,
      remainingAmount: BUY_AMOUNT,
      costBasisLamports: BUY_AMOUNT,
      entryTime: Date.now(),
      highestValue: 0,
      tiersHit: TP_TIERS.map(() => false),
      entryMarketCapUSD: mySnapshot.marketCapUSD,
    });

    await persistPositions();

    if (buySig) {
      const cleanSig = buySig.split(":")[1] || buySig;

      await appendTradeJournal({
        mint,
        action: "BUY",
        solAmount: BUY_AMOUNT_SOL,
        entryMarketCapUSD: mySnapshot.marketCapUSD,
        exitMarketCapUSD: null,
        pnlPercent: null,
        timestamp: Date.now(),
        signature: cleanSig,
      });

      await sendTelegramAlert(
        `🚀 <b>BUY EXECUTED</b>\n` +
        `Mint: <code>${mint}</code>\n` +
        `Amount: ${BUY_AMOUNT_SOL.toFixed(3)} SOL\n` +
        `<b>Target MC:</b> ${fmtMC(targetSnapshot.marketCapUSD)}\n` +
        `<b>Your MC:</b> ${fmtMC(mySnapshot.marketCapUSD)}\n` +
        `<a href="https://solscan.io/tx/${cleanSig}">View on Solscan</a>`
      );
    }
  } finally {
    inFlight.delete(mint);
  }
}

/* ================= SELL EXECUTION ================= */

async function executeSell(mint: string, amount: number, reason: string) {
  const pos = positions.get(mint);
  if (!pos || pos.remainingAmount <= 0) return;

  try {
    const quote = await getQuote(mint, SOL_MINT, amount);
    if (!quote) return;

    const swapTx = await buildSwapTx(quote);
    if (!swapTx) return;

    swapTx.sign([wallet]);
    const rawBytes = swapTx.serialize();

    const jitoPromise = sendJitoBundle(rawBytes);
    const rpcPromise = connection.sendRawTransaction(rawBytes, {
      skipPreflight: true,
      maxRetries: 1,
    }).then(sig => `rpc:${sig}`).catch(() => null);

    const sig = await Promise.race([jitoPromise, rpcPromise]);
    if (!sig) return;

    console.log(`✅ SELL (${reason}):`, mint);

    const soldFraction = amount / pos.remainingAmount;
    const costBasisSold = pos.costBasisLamports * soldFraction;
    const proceedsLamports = Number(quote.outAmount);
    const pnlPercent = ((proceedsLamports - costBasisSold) / costBasisSold) * 100;

    pos.remainingAmount -= amount;
    pos.costBasisLamports -= costBasisSold;

    if (pos.remainingAmount <= 0) positions.delete(mint);
    await persistPositions();

    const { marketCapUSD } = await fetchPriceAndMarketCap(mint);
    const cleanSig = sig.split(":")[1] || sig;

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
      `✅ <b>SELL</b> (${reason})\n` +
      `<code>${mint}</code>\n` +
      `MC: ${fmtMC(marketCapUSD)}\n` +
      `PnL: ${pnlPercent >= 0 ? "+" : ""}${pnlPercent.toFixed(1)}%\n` +
      `<a href="https://solscan.io/tx/${cleanSig}">View on Solscan</a>`
    );
  } catch (e: any) {
    console.log("Sell failed:", e.message);
  }
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

/* ================= TELEGRAM COMMANDS ================= */

async function pollTelegramCommands() {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;
  try {
    const url = `https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/getUpdates?offset=${telegramUpdateOffset}&timeout=0`;
    const data: any = await fetchJson(url);
    if (!data?.result) return;

    for (const update of data.result) {
      telegramUpdateOffset = update.update_id + 1;
      const msg = update.message;
      if (!msg || String(msg.chat?.id) !== String(TELEGRAM_CHAT_ID)) continue;

      const text = (msg.text ?? "").trim().toLowerCase();
      if (text === "/pause") {
        botPaused = true;
        await sendTelegramAlert("⏸️ <b>Bot paused.</b> Send /resume to continue.");
      } else if (text === "/resume") {
        botPaused = false;
        await sendTelegramAlert("▶️ <b>Bot resumed.</b>");
      } else if (text === "/status") {
        await sendTelegramAlert(`📊 <b>Status</b>\nPaused: ${botPaused}\nPositions: ${positions.size}`);
      }
    }
  } catch {}
}

/* ================= BALANCE CHECK ================= */

let lastLowBalanceAlertAt = 0;

async function checkWalletBalance() {
  try {
    const lamports = await connection.getBalance(wallet.publicKey, "confirmed");
    const sol = lamports / 1e9;
    if (sol < MIN_WALLET_BALANCE_SOL && Date.now() - lastLowBalanceAlertAt > 30 * 60 * 1000) {
      lastLowBalanceAlertAt = Date.now();
      await sendTelegramAlert(`⚠️ <b>LOW BALANCE</b>\n${sol.toFixed(4)} SOL remaining`);
    }
  } catch {}
}

/* ================= DETECTION ================= */

function getResolvedAccountKeys(tx: any): PublicKey[] {
  const staticKeys = tx.transaction?.message?.staticAccountKeys ?? [];
  const loaded = tx.meta?.loadedAddresses;
  return [...staticKeys, ...(loaded?.writable ?? []), ...(loaded?.readonly ?? [])];
}

async function getTransactionWithRetry(signature: string, attempts = 4, delayMs = 200) {
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

async function handleFallbackDetection(signature: string) {
  try {
    const tx = await getTransactionWithRetry(signature);
    if (!tx?.meta) return;

    const keys = getResolvedAccountKeys(tx);
    const targetIdx = keys.findIndex(k => k.equals(TARGET_WALLET));
    if (targetIdx === -1) return;

    const solDelta = (tx.meta.preBalances?.[targetIdx] ?? 0) - (tx.meta.postBalances?.[targetIdx] ?? 0);
    if (solDelta < MIN_BUY_SOL) return;

    for (const post of tx.meta.postTokenBalances ?? []) {
      if (post.owner !== TARGET_WALLET.toString() || post.mint === SOL_MINT) continue;
      const pre = tx.meta.preTokenBalances?.find(p => p.owner === post.owner && p.mint === post.mint);
      if (Number(post.uiTokenAmount.uiAmount ?? 0) > Number(pre?.uiTokenAmount.uiAmount ?? 0)) {
        console.log(`🔥 Detected: ${post.mint}`);
        executeBuy(post.mint, fetchPriceAndMarketCap(post.mint));
        break;
      }
    }
  } catch (e: any) {
    console.log("Detection error:", e.message);
  }
}

function onWalletLog(log: Logs) {
  if (alreadySeen(log.signature)) return;

  const fast = tryFastDecode(log.logs);
  if (fast) {
    if (fast.user !== TARGET_WALLET.toString() || !fast.isBuy || fast.solAmount < MIN_BUY_SOL) return;
    console.log(`⚡ FAST-PATH: ${fast.mint}`);
    executeBuy(fast.mint, fetchPriceAndMarketCap(fast.mint));
    return;
  }

  handleFallbackDetection(log.signature);
}

/* ================= WEBSOCKET ================= */

const subIds: (number | null)[] = detectionConnections.map(() => null);

function refreshSubscriptions() {
  detectionConnections.forEach((conn, i) => {
    const oldSubId = subIds[i] ?? null;
    subIds[i] = conn.onLogs(TARGET_WALLET, onWalletLog, "processed");
    if (oldSubId !== null) {
      setTimeout(() => conn.removeOnLogsListener(oldSubId).catch(() => {}), 3000);
    }
  });
  console.log(`✅ ${detectionConnections.length} subscription(s) refreshed`);
}

/* ================= PUMPPORTAL ================= */

const PUMPPORTAL_API_KEY = process.env.PUMPPORTAL_API_KEY ?? "";

function startPumpPortal() {
  const WS: any = (globalThis as any).WebSocket;
  if (!WS) return;

  const connect = () => {
    let ws: any;
    try {
      const wsUrl = PUMPPORTAL_API_KEY
        ? `wss://pumpportal.fun/api/data?api-key=${PUMPPORTAL_API_KEY}`
        : "wss://pumpportal.fun/api/data";
      ws = new WS(wsUrl);
    } catch {
      setTimeout(connect, 5000);
      return;
    }

    ws.onopen = () => {
      ws.send(JSON.stringify({ method: "subscribeAccountTrade", keys: [TARGET_WALLET.toString()] }));
    };

    ws.onmessage = (ev: any) => {
      try {
        const msg = JSON.parse(typeof ev.data === "string" ? ev.data : ev.data.toString());
        if (!msg?.signature || !msg?.mint) return;
        if (msg.traderPublicKey !== TARGET_WALLET.toString()) return;
        if (alreadySeen(msg.signature)) return;
        if (msg.txType !== "buy") return;

        const lamports = Number(msg.solAmount ?? 0) * 1e9;
        if (lamports < MIN_BUY_SOL) return;

        console.log("⚡ PUMPPORTAL:", msg.mint);
        executeBuy(msg.mint, fetchPriceAndMarketCap(msg.mint));
      } catch {}
    };

    ws.onclose = () => setTimeout(connect, 3000);
    ws.onerror = () => { try { ws.close(); } catch {} };
  };

  connect();
}

/* ================= POLLING ================= */

let lastPolledSignature: string | null = null;

async function initPollingCursor() {
  try {
    const sigs = await connection.getSignaturesForAddress(TARGET_WALLET, { limit: 1 }, "confirmed");
    if (sigs[0]) lastPolledSignature = sigs[0].signature;
  } catch {}
}

async function pollForMissedTrades() {
  try {
    const options: any = { limit: 25 };
    if (lastPolledSignature) options.until = lastPolledSignature;

    const sigs = await connection.getSignaturesForAddress(TARGET_WALLET, options, "confirmed");
    if (!sigs.length) return;

    for (const sigInfo of [...sigs].reverse()) {
      if (sigInfo.err || alreadySeen(sigInfo.signature)) continue;
      console.log("🔎 POLL found:", sigInfo.signature);
      await handleFallbackDetection(sigInfo.signature);
    }

    if (sigs[0]) lastPolledSignature = sigs[0].signature;
  } catch (e: any) {
    console.log("Polling error:", e.message);
  }
}

/* ================= SERVER ================= */

const app = express();
app.get("/", (_req, res) => res.send("Sub-second Engine Active"));
app.get("/health", (_req, res) => res.json({
  status: "ok",
  positions: positions.size,
  paused: botPaused,
  uptime: process.uptime(),
}));

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server on port ${PORT}`));

/* ================= START ================= */

async function start() {
  console.log("🚀 Sub-second copy-trade bot starting...");
  console.log("Target:", TARGET_WALLET.toString());
  console.log("Buy amount:", BUY_AMOUNT_SOL, "SOL");

  const balance = await connection.getBalance(wallet.publicKey);
  console.log("Wallet balance:", balance / 1e9, "SOL");

  await Promise.all([
    loadPositions(),
    initPumpDecoder(),
    initPollingCursor(),
    refreshBlockData(),
  ]);

  refreshSubscriptions();
  setInterval(refreshSubscriptions, 60 * 1000);
  setInterval(pollForMissedTrades, POLL_INTERVAL_MS);
  setInterval(refreshBlockData, 2000);
  setInterval(monitor, 1000);
  setInterval(checkWalletBalance, 60 * 1000);
  setInterval(pollTelegramCommands, 3000);

  if (PUMPPORTAL_ENABLED) startPumpPortal();

  console.log("✅ Bot running — Pump.fun direct: 60-200ms | Jupiter fallback: 400-800ms");
  await sendTelegramAlert(
    `🟢 <b>Sub-second Bot Online</b>\n` +
    `Target: <code>${TARGET_WALLET.toString()}</code>\n` +
    `Buy: ${BUY_AMOUNT_SOL} SOL\n` +
    `Balance: ${(balance / 1e9).toFixed(3)} SOL\n` +
    `Commands: /pause /resume /status`
  );
}

start().catch(console.error);