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
  sendAndConfirmTransaction,
} from "@solana/web3.js";
import type { Logs } from "@solana/web3.js";
import { Program, AnchorProvider, BorshEventCoder } from "@coral-xyz/anchor";
import { getAssociatedTokenAddress } from "@solana/spl-token";
import fetch from "node-fetch";
import bs58 from "bs58";
import https from "https";
import express from "express";
import crypto from "crypto";

/* ================= CONFIG ================= */

function requireEnv(name: string): string {
  const val = process.env[name];
  if (!val) throw new Error(`Missing required env var: ${name}`);
  return val;
}

const RPC_URL = requireEnv("RPC_URL");
const PRIVATE_KEY = requireEnv("PRIVATE_KEY");
const TARGET_WALLET = new PublicKey(requireEnv("TARGET_WALLET"));

const agent = new https.Agent({ 
  keepAlive: true, 
  maxSockets: 64,
  keepAliveMsecs: 1000,
});

function makeConnection(url: string): Connection {
  return new Connection(url, {
    commitment: "processed",
    wsEndpoint: url.replace("https", "wss"),
    httpAgent: agent,
  });
}

const connection = makeConnection(RPC_URL);
const wallet = Keypair.fromSecretKey(bs58.decode(PRIVATE_KEY));

// Multi-provider WebSocket fanout
const WS_FANOUT = Math.max(1, Number(process.env.WS_FANOUT ?? 3));
const EXTRA_WS_URLS = (process.env.EXTRA_WS_URLS ?? "")
  .split(",")
  .map((s) => s.trim())
  .filter(Boolean);

const detectionConnections: Connection[] = [connection];
for (let i = 1; i < WS_FANOUT; i++) detectionConnections.push(makeConnection(RPC_URL));
for (const url of EXTRA_WS_URLS) detectionConnections.push(makeConnection(url));

const broadcastConnection = makeConnection(
  process.env.BROADCAST_RPC_URL ?? RPC_URL
);

// Trade parameters
const BUY_AMOUNT = Number(process.env.BUY_AMOUNT_SOL ?? 0.008) * 1e9;
const BASE_PRIORITY_FEE = Number(process.env.PRIORITY_FEE_LAMPORTS ?? 500_000);
const SLIPPAGE_BPS = Number(process.env.SLIPPAGE_BPS ?? 1500);
const MIN_BUY_SOL = Number(process.env.MIN_BUY_SOL ?? 0.01) * 1e9;
const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS ?? 5000);

// Exit strategy
const TP_TIERS = [
  { tp: 7.0, sellFraction: 0.5 },
  { tp: 11.0, sellFraction: 0.3 },
  { tp: 15.0, sellFraction: 0.2 },
];
const SL_PCT = -0.35;
const TRAIL_DRAWDOWN = -0.2;
const MAX_HOLD_MS = 5 * 60 * 1000;

// Constants
const SOL_MINT = "So11111111111111111111111111111111111111112";
const PUMPFUN_PROGRAM = new PublicKey("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P");
const PUMP_GLOBAL = new PublicKey("4wTV1YmiEkRvAtNtsSGPtUrqRYQMe5SKy2uB4Jjaxnjf");
const PUMP_EVENT_AUTHORITY = new PublicKey("Ce6TQqeHC9p8KetsN6JsjHK7UTZk7nasjjnr7XxXp9F1");
const PUMP_FEE_RECIPIENT = new PublicKey("CebN5WGQ4jvEPvsVU4EoHEpgzq1VV7AbicJhtHTAcVUN");
const TOKEN_PROGRAM = new PublicKey("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA");
const ASSOCIATED_TOKEN_PROGRAM = new PublicKey("ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL");

const JUP_BASE = "https://api.jup.ag/swap/v1";
const JUP_API_KEY = process.env.JUP_API_KEY;

/* ================= TELEGRAM ================= */

const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN;
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID;

async function sendTelegramAlert(message: string) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) return;
  try {
    await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
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
  } catch {}
}

/* ================= JITO ================= */

const JITO_BLOCK_ENGINE_URL = "https://mainnet.block-engine.jito.wtf/api/v1/bundles";
const JITO_TIP_ACCOUNTS = [
  "96gYZGLnJYVFmbjzopPSU6QiEV5fGqZNyN9nmNhvrZU5",
  "HFqU5x63VTqvQss8hp11i4e3Kk5aQfvAfEMn7hPfqLfE",
  "Cw8CFyM9FkoMi7K7Crf6HNQqf4uEMzpKw6QNghXLvLkY",
  "ADaUMid9yfUytqMBgopwjb2DTLsD9A2CboQdKQACQgkP",
  "DfXygSm4jCyNCybVYYK6DwvWqjKee8pbDmJGcLWNDXjh",
  "ADuUkR4vqLUMWXxWoS2XLbJcPZsXgkLGEGjuQqBYbFp4",
  "DttWaMuVvTiduZRnguLF7jNxTgiMBZ1hyAfmJ2fkxRTF",
  "3AVi9Tg9Uo68tJfuwoKfqSZf3YCFhhViMEEWqBYWZMgY",
];
const JITO_TIP_LAMPORTS = Number(process.env.JITO_TIP_LAMPORTS ?? 2_000_000);

function randomTipAccount(): PublicKey {
  return new PublicKey(JITO_TIP_ACCOUNTS[Math.floor(Math.random() * JITO_TIP_ACCOUNTS.length)]!);
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

let pumpEventCoder: BorshEventCoder | null = null;

/* ================= PUMP.FUN IDL DECODER ================= */

async function initPumpDecoder() {
  try {
    const provider = new AnchorProvider(connection, {} as any, {});
    const idl = await Program.fetchIdl(PUMPFUN_PROGRAM, provider);
    if (idl) {
      pumpEventCoder = new BorshEventCoder(idl as any);
      console.log("✅ Pump.fun decoder ready");
    }
  } catch {}
}

function tryFastDecode(logs: string[]): { 
  mint: string; 
  isBuy: boolean; 
  solAmount: number; 
  user: string;
  tokenAmount: number;
} | null {
  if (!pumpEventCoder) return null;
  
  for (const log of logs) {
    if (!log.startsWith("Program data: ")) continue;
    try {
      const decoded = pumpEventCoder.decode(log.slice("Program data: ".length));
      if (decoded?.name === "TradeEvent") {
        const d: any = decoded.data;
        return {
          mint: d.mint.toString(),
          isBuy: Boolean(d.isBuy),
          solAmount: Number(d.solAmount ?? 0),
          tokenAmount: Number(d.tokenAmount ?? 0),
          user: d.user.toString(),
        };
      }
    } catch {}
  }
  return null;
}

/* ================= PRE-CACHED DATA ================= */

interface CachedBlockData {
  blockhash: string;
  lastValidBlockHeight: number;
  expiryTime: number;
  priorityFee: number;
}

let cachedBlockData: CachedBlockData = {
  blockhash: "",
  lastValidBlockHeight: 0,
  expiryTime: 0,
  priorityFee: BASE_PRIORITY_FEE,
};

async function refreshBlockData() {
  try {
    const [latestBlockhash, fees] = await Promise.all([
      connection.getLatestBlockhash("processed"),
      connection.getRecentPrioritizationFees().catch(() => []),
    ]);

    cachedBlockData.blockhash = latestBlockhash.blockhash;
    cachedBlockData.lastValidBlockHeight = latestBlockhash.lastValidBlockHeight;
    cachedBlockData.expiryTime = Date.now() + 60000;

    if (fees.length > 0) {
      const sorted = fees.sort((a, b) => a.prioritizationFee - b.prioritizationFee);
      const median = sorted[Math.floor(sorted.length / 2)]!.prioritizationFee;
      cachedBlockData.priorityFee = Math.max(BASE_PRIORITY_FEE, Math.floor(median * 1.2));
    }
  } catch (e: any) {
    console.log("Block data refresh failed:", e.message);
  }
}

// Refresh every 2 seconds for sub-second execution
setInterval(refreshBlockData, 2000);

/* ================= DIRECT PUMP.FUN BUY (SUB-SECOND PATH) ================= */

async function buildPumpFunBuyInstruction(
  mint: PublicKey,
  solAmount: bigint
): Promise<TransactionInstruction> {
  // Derive bonding curve
  const [bondingCurve] = PublicKey.findProgramAddressSync(
    [Buffer.from("bonding-curve"), mint.toBuffer()],
    PUMPFUN_PROGRAM
  );

  // Derive associated bonding curve token account
  const [associatedBondingCurve] = PublicKey.findProgramAddressSync(
    [bondingCurve.toBuffer(), TOKEN_PROGRAM.toBuffer(), mint.toBuffer()],
    ASSOCIATED_TOKEN_PROGRAM
  );

  // Get user's associated token account
  const userAta = await getAssociatedTokenAddress(mint, wallet.publicKey);

  // Build instruction with minimal data
  const data = Buffer.alloc(24);
  // Buy discriminator: 0x66063d1201daebea
  data.writeBigUInt64LE(BigInt("0x66063d1201daebea"), 0);
  // Amount
  data.writeBigUInt64LE(solAmount, 8);
  // Max SOL cost (0 = no limit for buys)
  data.writeBigUInt64LE(BigInt(0), 16);

  return new TransactionInstruction({
    programId: PUMPFUN_PROGRAM,
    keys: [
      { pubkey: PUMP_GLOBAL, isSigner: false, isWritable: false },
      { pubkey: PUMP_FEE_RECIPIENT, isSigner: false, isWritable: true },
      { pubkey: mint, isSigner: false, isWritable: false },
      { pubkey: bondingCurve, isSigner: false, isWritable: true },
      { pubkey: associatedBondingCurve, isSigner: false, isWritable: true },
      { pubkey: userAta, isSigner: false, isWritable: true },
      { pubkey: wallet.publicKey, isSigner: true, isWritable: true },
      { pubkey: SystemProgram.programId, isSigner: false, isWritable: false },
      { pubkey: TOKEN_PROGRAM, isSigner: false, isWritable: false },
      { pubkey: PUMP_EVENT_AUTHORITY, isSigner: false, isWritable: false },
      { pubkey: PUMPFUN_PROGRAM, isSigner: false, isWritable: false },
    ],
    data,
  });
}

async function executePumpFunDirectBuy(mint: string): Promise<boolean> {
  const startTime = Date.now();
  
  try {
    const mintPubkey = new PublicKey(mint);
    const buyAmount = BigInt(Math.floor(BUY_AMOUNT));

    // Build Pump.fun instruction (5-15ms)
    const buyIx = await buildPumpFunBuyInstruction(mintPubkey, buyAmount);

    // Create transaction with pre-cached blockhash (1-2ms)
    const tx = new Transaction({
      feePayer: wallet.publicKey,
      blockhash: cachedBlockData.blockhash,
      lastValidBlockHeight: cachedBlockData.lastValidBlockHeight,
    });

    // Add compute budget instructions
    tx.add(
      ComputeBudgetProgram.setComputeUnitLimit({ units: 200_000 }),
      ComputeBudgetProgram.setComputeUnitPrice({ 
        microLamports: cachedBlockData.priorityFee 
      }),
      buyIx
    );

    // Sign locally (2-5ms)
    tx.sign(wallet);
    const rawBytes = tx.serialize();

    // Send via Jito bundle + direct RPC simultaneously (50-150ms)
    const jitoPromise = sendJitoBundle(rawBytes).catch(() => null);
    const rpcPromise = connection.sendRawTransaction(rawBytes, {
      skipPreflight: true,
      maxRetries: 0,
      preflightCommitment: "processed",
    }).catch(() => null);

    // Race both submission methods
    const result = await Promise.race([jitoPromise, rpcPromise]);

    const elapsed = Date.now() - startTime;
    
    if (result) {
      console.log(`⚡ PUMP.FUN DIRECT BUY: ${mint} (${elapsed}ms)`);
      
      // Mark position immediately
      positions.set(mint, {
        mint,
        originalAmount: BUY_AMOUNT,
        remainingAmount: BUY_AMOUNT,
        costBasisLamports: BUY_AMOUNT,
        entryTime: Date.now(),
        highestValue: 0,
        tiersHit: TP_TIERS.map(() => false),
      });

      // Fire and forget - don't block
      persistPositions().catch(() => {});
      sendBuyAlert(mint, result).catch(() => {});

      return true;
    }

    console.log(`❌ Pump.fun direct buy failed after ${elapsed}ms`);
    return false;
    
  } catch (e: any) {
    console.log(`❌ Pump.fun direct buy error (${Date.now() - startTime}ms):`, e.message);
    return false;
  }
}

/* ================= JUPITER BUY (FALLBACK) ================= */

function jupHeaders(extra: Record<string, string> = {}) {
  return JUP_API_KEY ? { "x-api-key": JUP_API_KEY, ...extra } : extra;
}

async function getQuote(inputMint: string, outputMint: string, amount: number) {
  const url = `${JUP_BASE}/quote?inputMint=${inputMint}&outputMint=${outputMint}&amount=${Math.floor(amount)}&slippageBps=${SLIPPAGE_BPS}&onlyDirectRoutes=true&restrictIntermediateTokens=true`;
  
  const response = await fetch(url, { 
    headers: jupHeaders(),
    agent,
  });
  
  const data: any = await response.json();
  return data?.outAmount ? data : null;
}

async function buildSwapTx(quoteResponse: any): Promise<VersionedTransaction | null> {
  const response = await fetch(`${JUP_BASE}/swap`, {
    method: "POST",
    headers: jupHeaders({ "Content-Type": "application/json" }),
    body: JSON.stringify({
      quoteResponse,
      userPublicKey: wallet.publicKey.toString(),
      wrapAndUnwrapSol: true,
      prioritizationFeeLamports: cachedBlockData.priorityFee,
      dynamicComputeUnitLimit: true,
      dynamicSlippage: true,
    }),
    agent,
  });

  const data: any = await response.json();
  if (!data?.swapTransaction) return null;

  return VersionedTransaction.deserialize(
    Buffer.from(data.swapTransaction, "base64")
  );
}

async function executeJupiterBuy(mint: string): Promise<boolean> {
  const startTime = Date.now();
  
  try {
    const quote = await getQuote(SOL_MINT, mint, BUY_AMOUNT);
    if (!quote) return false;

    const swapTx = await buildSwapTx(quote);
    if (!swapTx) return false;

    // Override with pre-cached blockhash
    swapTx.message.recentBlockhash = cachedBlockData.blockhash;
    
    swapTx.sign([wallet]);
    const rawBytes = swapTx.serialize();

    // Dual submission
    const jitoPromise = sendJitoBundle(rawBytes).catch(() => null);
    const rpcPromise = connection.sendRawTransaction(rawBytes, {
      skipPreflight: true,
      maxRetries: 0,
    }).catch(() => null);

    const result = await Promise.race([jitoPromise, rpcPromise]);

    const elapsed = Date.now() - startTime;
    
    if (result) {
      console.log(`🪐 JUPITER BUY: ${mint} (${elapsed}ms)`);
      
      positions.set(mint, {
        mint,
        originalAmount: BUY_AMOUNT,
        remainingAmount: BUY_AMOUNT,
        costBasisLamports: BUY_AMOUNT,
        entryTime: Date.now(),
        highestValue: 0,
        tiersHit: TP_TIERS.map(() => false),
      });

      persistPositions().catch(() => {});
      sendBuyAlert(mint, result).catch(() => {});
      
      return true;
    }
    
    return false;
    
  } catch (e: any) {
    console.log(`Jupiter buy failed (${Date.now() - startTime}ms):`, e.message);
    return false;
  }
}

/* ================= UNIFIED BUY EXECUTION ================= */

async function executeBuy(mint: string) {
  if (positions.has(mint) || inFlight.has(mint)) return;
  
  inFlight.add(mint);
  
  try {
    // Try Pump.fun direct path first (60-200ms)
    const pumpSuccess = await executePumpFunDirectBuy(mint);
    
    if (!pumpSuccess) {
      // Fall back to Jupiter (400-800ms)
      console.log(`⚠️ Falling back to Jupiter for ${mint}`);
      await executeJupiterBuy(mint);
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
    // Use Jupiter for sells (more reliable for output)
    const quote = await getQuote(mint, SOL_MINT, amount);
    if (!quote) return;

    const swapTx = await buildSwapTx(quote);
    if (!swapTx) return;

    swapTx.message.recentBlockhash = cachedBlockData.blockhash;
    swapTx.sign([wallet]);

    const rawBytes = swapTx.serialize();
    
    const jitoPromise = sendJitoBundle(rawBytes).catch(() => null);
    const rpcPromise = connection.sendRawTransaction(rawBytes, {
      skipPreflight: true,
      maxRetries: 1,
    }).catch(() => null);

    const sig = await Promise.race([jitoPromise, rpcPromise]);
    if (!sig) return;

    console.log(`✅ SELL (${reason}):`, mint);

    pos.remainingAmount -= amount;
    if (pos.remainingAmount <= 0) {
      positions.delete(mint);
    }
    
    await persistPositions();
    
    const cleanSig = sig.toString().replace("rpc:", "");
    await sendTelegramAlert(
      `✅ <b>SELL</b> (${reason})\n` +
      `<code>${mint}</code>\n` +
      `<a href="https://solscan.io/tx/${cleanSig}">View on Solscan</a>`
    );
    
  } catch (e: any) {
    console.log(`Sell failed: ${e.message}`);
  }
}

/* ================= JITO BUNDLE ================= */

async function sendJitoBundle(rawTx: Uint8Array): Promise<string | null> {
  try {
    const tipTx = new Transaction({
      feePayer: wallet.publicKey,
      blockhash: cachedBlockData.blockhash,
      lastValidBlockHeight: cachedBlockData.lastValidBlockHeight,
    }).add(
      SystemProgram.transfer({
        fromPubkey: wallet.publicKey,
        toPubkey: randomTipAccount(),
        lamports: JITO_TIP_LAMPORTS,
      })
    );
    
    tipTx.sign(wallet);

    const bundle = [
      bs58.encode(rawTx),
      bs58.encode(tipTx.serialize()),
    ];

    const response = await fetch(JITO_BLOCK_ENGINE_URL, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        jsonrpc: "2.0",
        id: 1,
        method: "sendBundle",
        params: [bundle],
      }),
      agent,
    });

    const data: any = await response.json();
    return data?.result ? `jito:${data.result}` : null;
  } catch {
    return null;
  }
}

/* ================= ALERTS & PERSISTENCE ================= */

async function sendBuyAlert(mint: string, txSig: string) {
  try {
    const priceRes = await fetch(`https://api.jup.ag/price/v3?ids=${mint}`, {
      headers: jupHeaders(),
      agent,
    });
    const priceData: any = await priceRes.json();
    const mc = priceData?.[mint]?.usdPrice 
      ? `$${priceData[mint].usdPrice.toFixed(6)}` 
      : "unknown";

    const cleanSig = txSig.replace("jito:", "").replace("rpc:", "");
    await sendTelegramAlert(
      `🚀 <b>BUY</b>\n` +
      `<code>${mint}</code>\n` +
      `Size: ${(BUY_AMOUNT / 1e9).toFixed(3)} SOL\n` +
      `MC: ${mc}\n` +
      `<a href="https://solscan.io/tx/${cleanSig}">View on Solscan</a>`
    );
  } catch {}
}

async function persistPositions() {
  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!UPSTASH_URL || !UPSTASH_TOKEN) return;
  
  try {
    await fetch(`${UPSTASH_URL}/set/positions`, {
      agent,
      method: "POST",
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
      body: JSON.stringify(Array.from(positions.entries())),
    });
  } catch {}
}

async function loadPositions() {
  const UPSTASH_URL = process.env.UPSTASH_REDIS_REST_URL;
  const UPSTASH_TOKEN = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!UPSTASH_URL || !UPSTASH_TOKEN) return;
  
  try {
    const res = await fetch(`${UPSTASH_URL}/get/positions`, {
      agent,
      headers: { Authorization: `Bearer ${UPSTASH_TOKEN}` },
    });
    const data: any = await res.json();
    if (data?.result) {
      const entries = JSON.parse(data.result);
      for (const [mint, pos] of entries) positions.set(mint, pos);
      console.log(`✅ Restored ${entries.length} positions`);
    }
  } catch {}
}

/* ================= DETECTION ================= */

function alreadySeen(sig: string): boolean {
  if (seenSignatures.has(sig)) return true;
  seenSignatures.add(sig);
  if (seenSignatures.size > 10000) {
    const toDelete = Array.from(seenSignatures).slice(0, 5000);
    toDelete.forEach(s => seenSignatures.delete(s));
  }
  return false;
}

async function onWalletLog(log: Logs) {
  if (alreadySeen(log.signature)) return;

  // Try fast-path decode first
  const fast = tryFastDecode(log.logs);
  
  if (fast) {
    // Trust fast-path completely - no fallback
    if (fast.user !== TARGET_WALLET.toString()) return;
    if (!fast.isBuy) return;
    if (fast.solAmount < MIN_BUY_SOL) return;
    
    console.log(`⚡ PUMP.FUN DETECTED: ${fast.mint}`);
    
    // Execute immediately - don't await
    executeBuy(fast.mint);
    return;
  }

  // Fallback: get transaction details for non-Pump.fun trades
  console.log(`🔍 Fallback detection: ${log.signature}`);
  
  try {
    const tx = await getTransactionWithRetry(log.signature);
    if (!tx?.meta) return;

    const keys = getResolvedAccountKeys(tx);
    const targetIdx = keys.findIndex(k => k.equals(TARGET_WALLET));
    if (targetIdx === -1) return;

    const solDelta = (tx.meta.preBalances?.[targetIdx] ?? 0) - 
                    (tx.meta.postBalances?.[targetIdx] ?? 0);
    if (solDelta < MIN_BUY_SOL) return;

    // Find token balance increase
    for (const post of tx.meta.postTokenBalances ?? []) {
      if (post.owner !== TARGET_WALLET.toString() || post.mint === SOL_MINT) continue;
      
      const pre = tx.meta.preTokenBalances?.find(
        p => p.owner === post.owner && p.mint === post.mint
      );
      
      const preAmount = pre?.uiTokenAmount.uiAmount ?? 0;
      const postAmount = post.uiTokenAmount.uiAmount ?? 0;
      
      if (postAmount > preAmount) {
        console.log(`✅ Non-Pump.fun buy detected: ${post.mint}`);
        executeBuy(post.mint);
      }
    }
  } catch (e: any) {
    console.log(`Detection error: ${e.message}`);
  }
}

function getResolvedAccountKeys(tx: any): PublicKey[] {
  const staticKeys = tx.transaction?.message?.staticAccountKeys ?? [];
  const loaded = tx.meta?.loadedAddresses;
  return [...staticKeys, ...(loaded?.writable ?? []), ...(loaded?.readonly ?? [])];
}

async function getTransactionWithRetry(sig: string, retries = 4, delay = 200) {
  for (let i = 0; i < retries; i++) {
    const tx = await connection.getTransaction(sig, {
      commitment: "confirmed",
      maxSupportedTransactionVersion: 0,
    });
    if (tx?.meta) return tx;
    await new Promise(r => setTimeout(r, delay));
  }
  return null;
}

/* ================= MONITORING ================= */

async function monitorPositions() {
  for (const [mint, pos] of positions.entries()) {
    if (pos.remainingAmount <= 0) continue;

    try {
      const quote = await getQuote(mint, SOL_MINT, pos.remainingAmount);
      if (!quote) continue;

      const value = Number(quote.outAmount);
      const pnl = (value - pos.costBasisLamports) / pos.costBasisLamports;

      // Stop loss check first
      if (pnl <= SL_PCT) {
        await executeSell(mint, pos.remainingAmount, `SL ${(pnl*100).toFixed(0)}%`);
        continue;
      }

      // Max hold time
      if (Date.now() - pos.entryTime > MAX_HOLD_MS) {
        await executeSell(mint, pos.remainingAmount, "MAX_TIME");
        continue;
      }

      // Trailing stop (only if in profit)
      if (value > pos.costBasisLamports) {
        if (value > pos.highestValue) pos.highestValue = value;
        
        const drawdown = (value - pos.highestValue) / pos.highestValue;
        if (pos.highestValue > pos.costBasisLamports && drawdown < TRAIL_DRAWDOWN) {
          await executeSell(mint, pos.remainingAmount, "TRAILING");
          continue;
        }
      }

      // Tiered take profits
      for (let i = 0; i < TP_TIERS.length; i++) {
        if (pos.tiersHit[i]) continue;
        const tier = TP_TIERS[i]!;
        
        if (pnl >= tier.tp) {
          const sellAmount = Math.min(
            Math.floor(pos.originalAmount * tier.sellFraction),
            pos.remainingAmount
          );
          
          if (sellAmount > 0) {
            await executeSell(mint, sellAmount, `TP${i+1} +${(tier.tp*100).toFixed(0)}%`);
            pos.tiersHit[i] = true;
          }
        }
      }
    } catch {}
  }
}

/* ================= WEBSOCKET MANAGEMENT ================= */

const subIds: (number | null)[] = detectionConnections.map(() => null);

function refreshSubscriptions() {
  detectionConnections.forEach((conn, i) => {
    const oldSubId = subIds[i];
    subIds[i] = conn.onLogs(TARGET_WALLET, onWalletLog, "processed");
    
    if (oldSubId !== null) {
      setTimeout(() => {
        conn.removeOnLogsListener(oldSubId).catch(() => {});
      }, 3000);
    }
  });
  
  console.log(`✅ ${detectionConnections.length} subscriptions active`);
}

/* ================= POLLING BACKSTOP ================= */

let lastPolledSig: string | null = null;

async function initPolling() {
  try {
    const sigs = await connection.getSignaturesForAddress(TARGET_WALLET, { limit: 1 }, "confirmed");
    if (sigs.length > 0) {
      lastPolledSig = sigs[0]!.signature;
    }
  } catch {}
}

async function pollMissedTrades() {
  try {
    const options: any = { limit: 10 };
    if (lastPolledSig) options.until = lastPolledSig;

    const sigs = await connection.getSignaturesForAddress(TARGET_WALLET, options, "confirmed");
    if (sigs.length === 0) return;

    const ordered = [...sigs].reverse();

    for (const sigInfo of ordered) {
      if (sigInfo.err) continue;
      if (alreadySeen(sigInfo.signature)) continue;
      
      console.log(`🔎 Polling found: ${sigInfo.signature}`);
      
      const tx = await getTransactionWithRetry(sigInfo.signature);
      if (tx?.meta) {
        const keys = getResolvedAccountKeys(tx);
        const targetIdx = keys.findIndex(k => k.equals(TARGET_WALLET));
        if (targetIdx !== -1) {
          const solDelta = (tx.meta.preBalances?.[targetIdx] ?? 0) - 
                          (tx.meta.postBalances?.[targetIdx] ?? 0);
          if (solDelta >= MIN_BUY_SOL) {
            for (const post of tx.meta.postTokenBalances ?? []) {
              if (post.owner === TARGET_WALLET.toString() && post.mint !== SOL_MINT) {
                executeBuy(post.mint);
              }
            }
          }
        }
      }
    }

    lastPolledSig = sigs[0]?.signature ?? lastPolledSig;
  } catch {}
}

/* ================= SERVER ================= */

const app = express();
app.get("/", (_req, res) => res.send("Sub-second Engine Active"));
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Server on port ${PORT}`));

/* ================= STARTUP ================= */

async function start() {
  console.log("🚀 Sub-second copy-trade bot starting...");
  console.log(`Target: ${TARGET_WALLET.toString()}`);
  console.log(`Buy amount: ${(BUY_AMOUNT / 1e9).toFixed(3)} SOL`);
  
  await Promise.all([
    initPumpDecoder(),
    loadPositions(),
    initPolling(),
    refreshBlockData(),
  ]);

  refreshSubscriptions();
  
  // Refresh subscriptions every 60s
  setInterval(refreshSubscriptions, 60000);
  
  // Monitor positions every 500ms
  setInterval(monitorPositions, 500);
  
  // Poll for missed trades
  setInterval(pollMissedTrades, POLL_INTERVAL_MS);

  await sendTelegramAlert(
    `🟢 <b>Sub-second Bot Online</b>\n` +
    `Target: <code>${TARGET_WALLET.toString()}</code>\n` +
    `Buy: ${(BUY_AMOUNT / 1e9).toFixed(3)} SOL\n` +
    `Path: Pump.fun direct (60-200ms) + Jupiter fallback (400-800ms)`
  );

  console.log("✅ Bot running");
}

// Error handling
process.on("uncaughtException", async (err) => {
  console.log("💥 CRASH:", err.message);
  await sendTelegramAlert(`💥 <b>CRASH</b>\n${err.message}`);
});

process.on("unhandledRejection", async (reason: any) => {
  console.log("💥 REJECTION:", reason);
  await sendTelegramAlert(`💥 <b>ERROR</b>\n${String(reason).slice(0, 200)}`);
});

start();