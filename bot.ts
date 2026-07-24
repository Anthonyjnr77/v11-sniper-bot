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
import { getMint, TOKEN_2022_PROGRAM_ID, TOKEN_PROGRAM_ID } from "@solana/spl-token";
import { OnlinePumpSdk, PUMP_SDK, getBuyTokenAmountFromSolAmount } from "@pump-fun/pump-sdk";
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

const agent = new https.Agent({ keepAlive: true, maxSockets: 64 });

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
const MIN_BUY_SOL = Number(process.env.MIN_BUY_SOL ?? 0.01) * 1e9;
const POLL_INTERVAL_MS = Number(process.env.POLL_INTERVAL_MS ?? 5000);

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

const PUMPFUN_PROGRAM = new PublicKey("6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P");
const pumpSdk = new OnlinePumpSdk(connection);
const LOCAL_PUMP_COMPUTE_UNITS = 200_000;

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
  console.log("💥 UNCAUGHT EXCEPTION:", err.message);
  await sendTelegramAlert(`💥 <b>BOT CRASHED</b>\n${err.message}\nRender should auto-restart it.`);
});

process.on("unhandledRejection", async (reason: any) => {
  console.log("💥 UNHANDLED REJECTION:", reason);
  await sendTelegramAlert(`💥 <b>BOT ERROR</b>\n${String(reason).slice(0, 200)}`);
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
      console.log("⚠️ Could not fetch Pump.fun IDL — fast path disabled.");
      return;
    }
    pumpEventCoder = new BorshEventCoder(idl as any);
    console.log("✅ Pump.fun fast-path decoder ready");
  } catch (e: any) {
    console.log("⚠️ Pump.fun decoder init failed:", e.message);
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
const JITO_TIP_LAMPORTS = Number(process.env.JITO_TIP_LAMPORTS ?? 2_000_000);

function randomTipAccount(): PublicKey {
  const idx = Math.floor(Math.random() * JITO_TIP_ACCOUNTS.length);
  const addr = JITO_TIP_ACCOUNTS[idx] ?? JITO_TIP_ACCOUNTS[0];
  return new PublicKey(addr as string);
}

function logFeeSizingWarning() {
  const totalFeeLamports = JITO_TIP_LAMPORTS + BASE_PRIORITY_FEE;
  const pctOfBuy = (totalFeeLamports / BUY_AMOUNT) * 100;
  console.log(
    `💸 Fee sizing: Jito tip + priority fee ≈ ${(totalFeeLamports / 1e9).toFixed(4)} SOL ` +
    `(~${pctOfBuy.toFixed(1)}% of ${(BUY_AMOUNT / 1e9).toFixed(4)} SOL buy)`
  );
  console.log(
    `🎯 Effective slippage: ${(SLIPPAGE_BPS / 100).toFixed(0)}% (retry ${(RETRY_SLIPPAGE_BPS / 100).toFixed(0)}%) — ` +
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
  return Math.floor(BASE_PRIORITY_FEE * (1 + Math.random()));
}

function jupHeaders(extra: Record<string, string> = {}) {
  return JUP_API_KEY ? { "x-api-key": JUP_API_KEY, ...extra } : extra;
}

/* ================= BLOCKHASH CACHE ================= */

let cachedBlockhash: string | null = null;
let cachedBlockhashAt = 0;

async function refreshBlockhash() {
  try {
    const { blockhash } = await connection.getLatestBlockhash("processed");
    cachedBlockhash = blockhash;
    cachedBlockhashAt = Date.now();
  } catch {
    // keep the old one
  }
}

async function getBlockhashFast(): Promise<string> {
  if (cachedBlockhash && Date.now() - cachedBlockhashAt < 40_000) return cachedBlockhash;
  await refreshBlockhash();
  if (cachedBlockhash && Date.now() - cachedBlockhashAt < 40_000) return cachedBlockhash;
  return (await connection.getLatestBlockhash("processed")).blockhash;
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
        console.log(`⚠️ Wallet balance low: ${sol.toFixed(4)} SOL`);
        await sendTelegramAlert(
          `⚠️ <b>LOW WALLET BALANCE</b>\nCurrent: ${sol.toFixed(4)} SOL\nThreshold: ${MIN_WALLET_BALANCE_SOL} SOL`
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
// Delay them off the hot buy window — the alert only needs them later.
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
    console.log(`✅ Restored ${entries.length} position(s) from before restart`);
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
        console.log("⏸️ Bot PAUSED via Telegram command");
        await sendTelegramAlert("⏸️ <b>Bot paused.</b> No new buys will be taken. Existing positions still monitored/sold normally. Send /resume to continue.");
      } else if (text === "/resume") {
        botPaused = false;
        console.log("▶️ Bot RESUMED via Telegram command");
        await sendTelegramAlert("▶️ <b>Bot resumed.</b> New buys re-enabled.");
      } else if (text === "/status") {
        const balanceStr = lastKnownBalanceLamports !== null
          ? `${(lastKnownBalanceLamports / 1e9).toFixed(4)} SOL`
          : "unknown";
        await sendTelegramAlert(
          `📊 <b>Status</b>\n` +
          `Paused: ${botPaused ? "YES" : "no"}\n` +
          `Open positions: ${positions.size}\n` +
          `Wallet: ${balanceStr}\n` +
          `WS channels: ${DETECTION_URLS.length}\n` +
          `PumpPortal: ${pumpPortalConnected ? "✅ connected" : "❌ NOT connected"}`
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
    console.log("❌ Quote failed after retries:", String(lastError).slice(0, 140));
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

async function submitSignedTransaction(rawBytes: Uint8Array): Promise<string | null> {
  const signed = VersionedTransaction.deserialize(rawBytes);
  const signature = bs58.encode(signed.signatures[0]!);
  const jitoAttempt = (async () => {
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
  const rpcAttempt = connection.sendRawTransaction(rawBytes, { skipPreflight: true, maxRetries: 3 }).then(() => "RPC");
  scheduleRebroadcasts(rawBytes);
  try {
    const via = await Promise.any([jitoAttempt, rpcAttempt]);
    console.log(`📤 Transaction submitted via ${via}:`, signature);
    return signature;
  } catch { return null; }
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
    const [mintInfo, global, latest] = await Promise.all([
      connection.getAccountInfo(mintKey, "processed"),
      pumpSdk.fetchGlobal(),
      connection.getLatestBlockhash("processed"),
    ]);
    if (!mintInfo) throw new Error("mint account not found");
    const tokenProgram = mintInfo.owner;
    if (!tokenProgram.equals(TOKEN_PROGRAM_ID) && !tokenProgram.equals(TOKEN_2022_PROGRAM_ID)) {
      throw new Error("unsupported token program");
    }

    const [buyState, mintState, feeConfig] = await Promise.all([
      pumpSdk.fetchBuyState(mintKey, wallet.publicKey, tokenProgram),
      getMint(connection, mintKey, "processed", tokenProgram),
      pumpSdk.fetchFeeConfig(),
    ]);
    const solAmount = new BN(BUY_AMOUNT);
    const tokenAmount = getBuyTokenAmountFromSolAmount({
      global,
      feeConfig,
      mintSupply: mintState.supply,
      bondingCurve: buyState.bondingCurve,
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
    const priorityMicroLamports = Math.ceil((BASE_PRIORITY_FEE * 1_000_000) / LOCAL_PUMP_COMPUTE_UNITS);
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
    console.log("↪️ Local Pump.fun build unavailable:", e.message);
    return null;
  }
}

// PumpPortal builds the current, correct Pump.fun transaction server-side —
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
        priorityFee: BASE_PRIORITY_FEE / 1e9,
        pool,
      }),
    });
    if (!res.ok) {
      const errText = await res.text().catch(() => "");
      console.log(`⚠️ trade-local ${action} build failed:`, String(errText).slice(0, 140));
      return null;
    }
    const buf = new Uint8Array(await res.arrayBuffer());
    const tx = VersionedTransaction.deserialize(buf);
    tx.sign([wallet]);
    return tx.serialize();
  } catch (e: any) {
    console.log(`⚠️ trade-local ${action} error:`, e.message);
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
          console.log("❌ Buy transaction failed on-chain:", signature, JSON.stringify(status.err));
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
      `❌ Quote rejected — price impact ${(impact * 100).toFixed(1)}% above ${(MAX_PRICE_IMPACT * 100).toFixed(0)}% cap`
    );
    return false;
  }
  return Number(quote.outAmount) > 0;
}

/* ================= BUY ================= */
// Direct Pump.fun is now tried FIRST — no Jupiter round trip for tokens still
// on the bonding curve, which is every real trade from this target wallet.
// Jupiter only runs as a fallback if the curve read fails or the token has
// already migrated off the bonding curve.

async function submitJupiterBuy(mint: string): Promise<string | null> {
  const quote = await getQuote(SOL_MINT, mint, BUY_AMOUNT);
  if (!quote || !passesFilters(quote)) return null;
  const tx = await buildSwapTx(quote);
  if (!tx) return null;
  const sig = await sendSwapDual(tx);
  if (sig) console.log("🚀 BUY submitted (Jupiter):", mint, sig);
  return sig;
}

async function executeBuy(
  mint: string,
  targetSnapshotPromise?: Promise<{ tokenPriceUSD: number | null; marketCapUSD: number | null }>
) {
  if (botPaused) {
    console.log("⏸️ Buy skipped — bot is paused:", mint);
    return;
  }

  const requiredLamports = BUY_AMOUNT + JITO_TIP_LAMPORTS + 3_000_000;
  if (lastKnownBalanceLamports !== null && lastKnownBalanceLamports < requiredLamports) {
    console.log(
      `⏸️ Buy skipped — wallet ${(lastKnownBalanceLamports / 1e9).toFixed(4)} SOL below required ~${(requiredLamports / 1e9).toFixed(4)} SOL:`,
      mint
    );
    return;
  }

  if (positions.has(mint) || inFlight.has(mint)) return;
  inFlight.add(mint);

  try {
    let sig: string | null = null;

    // 1. Primary: local official Pump SDK build. No external builder
    // latency, and the curve accounts are DERIVED from the mint — immune
    // by construction to the mint/bonding-curve mismatch (error 6004) a
    // stale external builder can produce on a seconds-old token. When the
    // curve state is one slot behind ("zero tokens"), one quick second
    // look usually succeeds.
    let rawTx = await buildLocalPumpBuyTx(mint);
    if (!rawTx) {
      await new Promise((r) => setTimeout(r, 300));
      rawTx = await buildLocalPumpBuyTx(mint);
    }
    if (rawTx) {
      sig = await sendRawTransactionDual(rawTx);
      if (sig) {
        console.log("🚀 BUY submitted (local Pump.fun):", mint, sig);
      }
    }

    // 2. Compatibility fallback. Ask for the bonding-curve pool EXPLICITLY
    // first: "auto" makes the builder race its own index on brand-new
    // tokens — that race is what produced the 6004 rejection.
    if (!sig) {
      console.log("↪️ Local Pump.fun build unavailable — trying PumpPortal for", mint);
      rawTx = await buildPumpPortalTx("buy", mint, BUY_AMOUNT / 1e9, true, SLIPPAGE_BPS, "pump");
      if (!rawTx) rawTx = await buildPumpPortalTx("buy", mint, BUY_AMOUNT / 1e9, true, SLIPPAGE_BPS, "auto");
      if (rawTx) {
        sig = await sendRawTransactionDual(rawTx);
        if (sig) {
          console.log("🚀 BUY submitted (PumpPortal-built):", mint, sig);
        }
      }
    }

    // 3. Last fallback for migrated or unsupported routes.
    if (!sig) {
      console.log("↪️ Pump.fun build unavailable — trying Jupiter for", mint);
      sig = await submitJupiterBuy(mint);
    }

    if (!sig) {
      console.log("❌ Buy aborted — both PumpPortal and Jupiter failed for", mint);
      return;
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
        if (sig) console.log("🚀 BUY retry submitted (fresh build, higher slippage):", mint, sig);
      }
      if (!sig) {
        console.log("↪️ Rebuild unavailable — trying Jupiter for", mint);
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

  let sig: string | null = null;

  const quote = await getQuote(mint, SOL_MINT, amount, 3);
  if (quote) {
    const tx = await buildSwapTx(quote);
    if (tx) sig = await sendSwapDual(tx);
  }

  // Fallback for tokens Jupiter cannot quote yet (seconds old): sell via a
  // PumpPortal-built transaction. Pump.fun tokens use 6 decimals.
  if (!sig) {
    console.log("↪️ Jupiter sell unavailable — trying PumpPortal build for", mint);
    const rawTx = await buildPumpPortalTx("sell", mint, amount / 1e6, false);
    if (rawTx) sig = await sendRawTransactionDual(rawTx);
  }

  if (!sig) {
    console.log(`❌ Sell failed on both paths for ${mint} (${reason})`);
    return;
  }

  console.log(`✅ SELL (${reason}):`, mint, sig);

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
    `✅ <b>SELL EXECUTED</b> (${reason})\n` +
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

  console.log(`🔄 Retry #${entry.attempts} for ${signature}`);
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
    console.log(`❌ PERMANENTLY MISSED TRADE: ${signature}`);
    await sendTelegramAlert(
      `🚨 <b>POSSIBLE MISSED TRADE</b>\nSignature: <code>${signature}</code>\nUnable to fetch after 5 retries. Check Solscan manually.`
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
          console.log("🚨 TARGET EXITED — dumping:", pre.mint);
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
        console.log("🔥 DETECTED buy:", post.mint);
        executeBuy(post.mint, delayedMarketCapSnapshot(post.mint));
        return true;
      }
    }

    if (!foundAny) {
      console.log("ℹ️ No token increase for target — not a buy we can copy.");
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
      console.log("❌ Could not fetch tx for", signature, "— queueing for retry");
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

  console.log(`👀 Activity: https://solscan.io/tx/${signature}`);

  const fast = tryFastDecode(log.logs);
  const isTargetTrade = fast && (() => {
    try { return new PublicKey(fast.user).equals(TARGET_WALLET); } catch { return false; }
  })();

  if (isTargetTrade && fast.isBuy && fast.solAmount >= MIN_BUY_SOL) {
    console.log(`⚡ IDL FAST-PATH BUY: ${fast.mint}`);
    seenSignatures.add(signature);
    executeBuy(fast.mint, delayedMarketCapSnapshot(fast.mint));
    return;
  }
  if (isTargetTrade && !fast.isBuy) {
    const held = positions.get(fast.mint);
    if (held && held.remainingAmount > 0) {
      console.log("🚨 Fast-path: target SOLD a token we hold — dumping:", fast.mint);
      seenSignatures.add(signature);
      executeSell(fast.mint, held.remainingAmount, "TARGET_EXITED");
      return;
    }
  }

  if (fast && !isTargetTrade) {
    console.log("↪️ Pump.fun trade event belongs to another wallet — racing RPC transaction fetch");
  } else if (fast && fast.isBuy && fast.solAmount < MIN_BUY_SOL) {
    console.log("↪️ Pump.fun buy is below MIN_BUY_SOL — racing RPC transaction fetch");
  } else {
    console.log("↪️ No direct Pump.fun TradeEvent — racing RPC transaction fetch");
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
      console.log("⚠️ Failed to open detection channel:", e.message);
    }
  }

  if (fresh.length === 0) {
    console.log("⚠️ Rotation produced no channels — keeping the old ones alive");
    return;
  }

  const old = activeChannels;
  activeChannels = fresh;

  if (old.length > 0) {
    setTimeout(() => {
      for (const ch of old) {
        ch.conn.removeOnLogsListener(ch.subId).catch(() => {});
        setTimeout(() => {
          try {
            (ch.conn as any)._rpcWebSocket?.close();
          } catch {}
        }, 2000);
      }
    }, 3000);
  }

  console.log(`✅ ${fresh.length} detection channel(s) rotated onto fresh websockets`);
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
    console.log("⚠️ PumpPortal channel DISABLED — no WebSocket support. Set NODE_VERSION=22 on Render, or run: npm i ws");
    await sendTelegramAlert(
      "⚠️ <b>PumpPortal channel is OFF</b>\nThis Node version has no WebSocket support. Set NODE_VERSION=22 in Render environment settings (or npm i ws) — this is your fastest detection channel."
    );
    return;
  }

  const connect = () => {
    let ws: any;
    try {
      ws = new WS(PUMPPORTAL_WS);
    } catch {
      setTimeout(connect, 5000);
      return;
    }

    ws.onopen = () => {
      pumpPortalConnected = true;
      console.log("✅ PumpPortal channel connected");
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
            console.log("🚨 PumpPortal: target SOLD a token we hold — dumping:", msg.mint);
            seenSignatures.add(msg.signature);
            executeSell(msg.mint, held.remainingAmount, "TARGET_EXITED");
          }
          return;
        }
        if (msg.txType !== "buy") return;

        const lamports = Number(msg.solAmount ?? 0) * 1e9;
        if (lamports < MIN_BUY_SOL) {
          console.log(`⏭️ PumpPortal: buy below threshold (${Number(msg.solAmount).toFixed(4)} SOL) — ignoring`);
          return;
        }

        console.log("⚡ PUMPPORTAL detected buy:", msg.mint);
        seenSignatures.add(msg.signature);
        executeBuy(msg.mint, delayedMarketCapSnapshot(msg.mint));
      } catch {
        // non-JSON frame, ignore
      }
    };

    ws.onclose = () => {
      pumpPortalConnected = false;
      console.log("⚠️ PumpPortal channel closed — reconnecting in 3s");
      setTimeout(connect, 3000);
    };

    ws.onerror = () => {
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

    const ordered = [...sigs].reverse();

    for (const sigInfo of ordered) {
      if (sigInfo.err) continue;
      if (seenSignatures.has(sigInfo.signature)) continue;
      console.log("🔎 POLL found unprocessed signature:", sigInfo.signature);
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
  console.log("🚀 Copy-trade bot starting. Target:", TARGET_WALLET.toString());

  logFeeSizingWarning();

  await Promise.all([loadPositions(), initPumpDecoder(), initPollingCursor(), refreshBlockhash()]);

  rotateSubscriptions();
  setInterval(rotateSubscriptions, 60 * 1000);
  setInterval(pollForMissedTrades, POLL_INTERVAL_MS);
  setInterval(refreshBlockhash, 25_000);
  setInterval(checkWalletBalance, 60 * 1000);
  setInterval(pollTelegramCommands, 3000);

  setInterval(() => {
    fetch("https://api.jup.ag/", { agent } as any).catch(() => {});
  }, 45_000);

  if (PUMPPORTAL_ENABLED) await startPumpPortal();

  await checkWalletBalance();

  console.log("✅ Bot fully running (rotating WS + PumpPortal + polling + PumpPortal-built buys)");
  await sendTelegramAlert(
    `🟢 <b>V25 Engine Online</b>\nTarget: <code>${TARGET_WALLET.toString()}</code>\n` +
    `Buy path: local Pump SDK → PumpPortal (pump pool) → Jupiter\n` +
    `Exit: TARGET-EXIT mirror + TP 700/1100/1500% + time net 15/25/40s\n` +
    `Channels: ${DETECTION_URLS.length} rotating WS + ${PUMPPORTAL_ENABLED ? "PumpPortal" : "no PumpPortal"} + polling\n` +
    `Commands: /pause /resume /status`
  );
}

start();