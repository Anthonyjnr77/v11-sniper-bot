import "dotenv/config";
import {
  Connection,
  Keypair,
  VersionedTransaction,
  PublicKey,
} from "@solana/web3.js";
import fetch from "node-fetch";
import bs58 from "bs58";
import https from "https";

/* ================= THE CONFIGURATION MATRIX ================= */

const RPC_URL = process.env.RPC_URL!;
const connection = new Connection(RPC_URL, {
  commitment: "processed",
  wsEndpoint: RPC_URL.replace("https", "wss"),
});

const wallet = Keypair.fromSecretKey(bs58.decode(process.env.PRIVATE_KEY!));
const TARGET_WALLET = new PublicKey(process.env.TARGET_WALLET!);

// Execution bounds
const BUY_AMOUNT = Number(process.env.BUY_AMOUNT_SOL) * 1e9;
const BASE_PRIORITY_FEE = Number(process.env.PRIORITY_FEE_LAMPORTS ?? 3_000_000);
const SLIPPAGE_BPS = Number(process.env.SLIPPAGE_BPS ?? 1500); // 15% slippage

const SOL_MINT = "So11111111111111111111111111111111111111112";
const JUP_BASE = "https://quote-api.jup.ag/v6";

// Tactical Exit Strategy
const TP_TIERS = [
  { tp: 7.0, sellFraction: 0.5 },   // At +700%, sell 50% of the bag
  { tp: 11.0, sellFraction: 0.3 },  // At +1100%, sell 30% of the bag
  { tp: 15.0, sellFraction: 0.2 },  // At +1500%, sell the remaining 20%
];
const SL_PCT = -0.35;         // Hard panic stop at -35%
const TRAIL_DRAWDOWN = -0.2;  // 20% trailing stop (activates only in profit)
const MAX_HOLD_MS = 5 * 60 * 1000; // Time-based exit limit (5 minutes)

/* ================= NETWORK LAYER ================= */

// Keep-Alive agent prevents network handshake delays on rapid requests
const agent = new https.Agent({ keepAlive: true });

async function fetchJson(url: string, options: any = {}) {
  return fetch(url, { agent, ...options }).then((r: any) => r.json());
}

// Jittered priority fee to avoid block bundling collisions
function getPriorityFee() {
  return Math.floor(BASE_PRIORITY_FEE * (1 + Math.random()));
}

/* ================= POSITION STATE MACHINE ================= */

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

/* ================= JUPITER ROUTING ================= */

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
      wrapAndUnwrapSol: true, // Crucial correct key for Jupiter v6
      prioritizationFeeLamports: getPriorityFee(),
      dynamicComputeUnitLimit: true,
    }),
  });
  if (!data || data.error || !data.swapTransaction) {
    console.log("❌ Swap build error:", data?.error ?? "unknown");
    return null;
  }
  return data.swapTransaction;
}

async function sendSwap(txBase64: string) {
  try {
    const tx = VersionedTransaction.deserialize(Buffer.from(txBase64, "base64"));
    tx.sign([wallet]);
    return await connection.sendRawTransaction(tx.serialize(), {
      skipPreflight: true,
      maxRetries: 3,
    });
  } catch (e: any) {
    console.log("❌ Send error:", e.message);
    return null;
  }
}

/* ================= ASYNC BALANCE VERIFICATION ================= */

async function getTokenBalance(mint: string): Promise<number> {
  try {
    const accs = await connection.getParsedTokenAccountsByOwner(wallet.publicKey, {
      mint: new PublicKey(mint),
    });
    return Number(accs.value[0]?.account.data.parsed.info.tokenAmount.amount ?? 0);
  } catch {
    return 0;
  }
}

// Retries checking the balance to prevent race conditions where processed blocks lag
async function waitForBalance(mint: string, attempts = 6, delayMs = 500): Promise<number> {
  for (let i = 0; i < attempts; i++) {
    const bal = await getTokenBalance(mint);
    if (bal > 0) return bal;
    await new Promise((r) => setTimeout(r, delayMs));
  }
  return 0;
}

/* ================= HOT PATH: BUY EXECUTION ================= */

function passesFilters(quote: any) {
  const impact = Number(quote.priceImpactPct ?? 0);
  if (impact > 0.15) {
    console.log(`⏭️ Skipped: Price impact too high (${(impact * 100).toFixed(2)}%)`);
    return false;
  }
  if (Number(quote.outAmount) <= 0) return false;
  return true;
}

async function executeBuy(mint: string) {
  if (positions.has(mint) || inFlight.has(mint)) return;
  inFlight.add(mint);

  try {
    const quote = await getQuote(SOL_MINT, mint, BUY_AMOUNT);
    if (!quote || !passesFilters(quote)) return;

    const tx = await buildSwapTx(quote);
    if (!tx) return;

    const sig = await sendSwap(tx);
    if (!sig) return;

    console.log(`🚀 BUY Executed | Mint: ${mint} | Sig: ${sig}`);

    // Wait for the on-chain balance to confirm before tracking
    const actual = await waitForBalance(mint);
    if (actual <= 0) {
      console.log(`⚠️ Balance unconfirmed for ${mint}. Manual intervention may be required.`);
      return;
    }

    // Register into the Position State Machine
    positions.set(mint, {
      mint,
      originalAmount: actual,
      remainingAmount: actual,
      costBasisLamports: BUY_AMOUNT,
      entryTime: Date.now(),
      highestValue: 0, 
      tiersHit: TP_TIERS.map(() => false),
    });
    
    console.log(`🎯 Position Tracked: ${actual} tokens secured.`);
  } finally {
    inFlight.delete(mint);
  }
}

/* ================= COLD PATH: SELL EXECUTION ================= */

async function executeSell(mint: string, amount: number, reason: string) {
  const pos = positions.get(mint);
  if (!pos) return;

  const quote = await getQuote(mint, SOL_MINT, amount);
  if (!quote) return;

  const tx = await buildSwapTx(quote);
  if (!tx) return;

  const sig = await sendSwap(tx);
  if (!sig) return;

  console.log(`✅ SELL Executed (${reason}) | Mint: ${mint} | Sig: ${sig}`);

  const soldFraction = amount / pos.remainingAmount;
  pos.remainingAmount -= amount;
  pos.costBasisLamports -= pos.costBasisLamports * soldFraction;

  if (pos.remainingAmount <= 0) {
    positions.delete(mint);
    console.log(`🏁 Position fully closed for ${mint}.`);
  }
}

/* ================= BACKGROUND RISK MONITOR ================= */

async function monitor() {
  for (const [mint, pos] of positions.entries()) {
    if (pos.remainingAmount <= 0) continue;

    getQuote(mint, SOL_MINT, pos.remainingAmount).then((quote) => {
      if (!quote) return;

      const value = Number(quote.outAmount);
      const pnl = (value - pos.costBasisLamports) / pos.costBasisLamports;

      // 1. Hard Stop Loss (Checked independently)
      if (pnl <= SL_PCT) {
        executeSell(mint, pos.remainingAmount, `Hard SL ${(pnl * 100).toFixed(0)}%`);
        return;
      }

      // 2. Time-Based Ejection
      if (Date.now() - pos.entryTime > MAX_HOLD_MS) {
        executeSell(mint, pos.remainingAmount, "Max Hold Time Exceeded");
        return;
      }

      // 3. Dynamic Trailing Stop (Only activates once the position hits true profit)
      if (value > pos.costBasisLamports) {
        if (value > pos.highestValue) pos.highestValue = value;

        const drawdown = (value - pos.highestValue) / pos.highestValue;
        if (pos.highestValue > pos.costBasisLamports && drawdown < TRAIL_DRAWDOWN) {
          executeSell(mint, pos.remainingAmount, `Trailing Stop (${(drawdown * 100).toFixed(0)}% from peak)`);
          return;
        }
      }

      // 4. Fractional Take Profit Tiers
      for (let i = 0; i < TP_TIERS.length; i++) {
        if (pos.tiersHit[i]) continue;
        
        if (pnl >= TP_TIERS[i].tp) {
          const sellAmount = Math.min(
            Math.floor(pos.originalAmount * TP_TIERS[i].sellFraction),
            pos.remainingAmount
          );
          
          if (sellAmount > 0) {
            executeSell(mint, sellAmount, `TP Tier ${i + 1} Hit (+${(TP_TIERS[i].tp * 100).toFixed(0)}%)`);
            pos.tiersHit[i] = true;
          }
        }
      }
    });
  }
}

// Decoupled monitor loop polling every 1.5 seconds
setInterval(monitor, 1500);

/* ================= ZERO-LATENCY EVENT LISTENER ================= */

async function handleTx(signature: string) {
  if (seenSignatures.has(signature)) return;
  seenSignatures.add(signature);

  try {
    // FIXED: Swapped 'processed' to 'confirmed' finality target to satisfy the Solana API requirements
    const tx = await connection.getTransaction(signature, {
      commitment: "confirmed",
      maxSupportedTransactionVersion: 0,
    });
    if (!tx?.meta) return;

    for (const b of tx.meta.postTokenBalances ?? []) {
      if (b.owner !== TARGET_WALLET.toString()) continue;
      if (b.mint === SOL_MINT) continue;

      console.log(`🔥 Signal Detected: Wallet purchased ${b.mint}`);
      executeBuy(b.mint);
    }
  } catch {}
}

/* ================= SYSTEM BOOT ================= */

console.log("=========================================");
console.log("🚀 V11 Quantitative Execution Engine Armed");
console.log(`📡 Tracking Target: ${TARGET_WALLET.toString()}`);
console.log("=========================================");

connection.onLogs(TARGET_WALLET, (log) => handleTx(log.signature), "processed");
/* ================= CLOUD DISGUISE ================= */
import express from "express";
const app = express();
app.get("/", (req, res) => res.send("V11 Engine is Live"));
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => console.log(`Web server listening on port ${PORT}`));