/**
 * Yellowstone gRPC Detection Worker
 *
 * Connects to Shyft gRPC endpoint and streams Pump.fun buy/sell events
 * for target wallets. Runs in a worker thread to avoid blocking main event loop.
 *
 * Protocol: Based on Bitquery/Shyft Pump.fun gRPC format
 * - Program ID: 6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P
 * - Events contain Buy/Sell with mint, user, amounts
 */

import { Worker, isMainThread, parentPort, workerData } from "worker_threads";
import type { SubscribeRequest, ClientDuplexStream } from "@triton-one/yellowstone-grpc";

/* ================= TYPES ================= */

export interface GrpcConfig {
  endpoint: string;
  token: string;
  targetWallets: string[];
  pumpFunProgramId: string;
}

export interface GrpcTradeEvent {
  type: "buy" | "sell";
  mint: string;
  user: string;
  solAmount: number;      // lamports
  tokenAmount: number;    // raw token amount (6 decimals for Pump.fun)
  signature: string;
  slot: number;
  timestamp: number;
}

export type WorkerMessage =
  | { type: "ready" }
  | { type: "trade"; event: GrpcTradeEvent }
  | { type: "error"; message: string }
  | { type: "reconnecting" }
  | { type: "connected" }
  | { type: "disconnected" };

/* ================= CONSTANTS ================= */

const PUMP_FUN_PROGRAM_ID = "6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P";
const RECONNECT_DELAY_MS = 3000;
const MAX_RECONNECT_DELAY_MS = 30000;
const PING_INTERVAL_MS = 20000;

/* ================= HELPER: Parse Shyft/Bitquery Trade Event ================= */

function parseTradeEvent(tx: any, slot: number): GrpcTradeEvent | null {
  try {
    // Shyft/Bitquery format: tx contains transaction with Buy/Sell events
    // Based on Bitquery docs: each trade message contains both Buy and Sell sides

    const events = tx?.events;
    if (!events) return null;

    // Look for Pump.fun trade events
    const pumpEvents = events?.pumpfun;
    if (!pumpEvents) return null;

    // pumpEvents can have buy and/or sell
    const buy = pumpEvents.buy;
    const sell = pumpEvents.sell;

    if (buy) {
      return {
        type: "buy",
        mint: buy.currency?.mintAddress ?? "",
        user: buy.account?.address ?? "",
        solAmount: Number(sell?.amount ?? 0),  // SOL spent (from sell side)
        tokenAmount: Number(buy.amount ?? 0),  // tokens received
        signature: tx.signature ?? "",
        slot,
        timestamp: Date.now(),
      };
    }

    if (sell) {
      return {
        type: "sell",
        mint: sell.currency?.mintAddress ?? "",
        user: sell.account?.address ?? "",
        solAmount: Number(buy?.amount ?? 0),   // SOL received (from buy side)
        tokenAmount: Number(sell.amount ?? 0), // tokens spent
        signature: tx.signature ?? "",
        slot,
        timestamp: Date.now(),
      };
    }

    return null;
  } catch (e) {
    return null;
  }
}

/* ================= WORKER THREAD LOGIC ================= */

if (!isMainThread && parentPort) {
  const config: GrpcConfig = workerData;

  let client: any = null;
  let stream: ClientDuplexStream | null = null;
  let reconnectAttempts = 0;
  let pingInterval: NodeJS.Timeout | null = null;
  let isShuttingDown = false;

  const targetWalletSet = new Set(config.targetWallets.map(w => w.toLowerCase()));
  const pumpFunProgramId = config.pumpFunProgramId ?? PUMP_FUN_PROGRAM_ID;

  function send(msg: WorkerMessage) {
    parentPort!.postMessage(msg);
  }

  function buildSubscribeRequest(): SubscribeRequest {
    return {
      accounts: {},
      slots: {},
      transactions: {
        pumpfun: {
          vote: false,
          failed: false,
          accountInclude: [pumpFunProgramId],
          accountRequired: [pumpFunProgramId],
          accountExclude: [],
        },
      },
      transactionsStatus: {},
      blocks: {},
      blocksMeta: {},
      entry: {},
      accountsDataSlice: [],
    };
  }

  async function connectAndSubscribe() {
    if (isShuttingDown) return;

    try {
      // Create gRPC client with Shyft endpoint
      const mod: any = await import("@triton-one/yellowstone-grpc");
      const Client = mod.default;
      client = new Client(config.endpoint, config.token, undefined, {
        enabled: true,
        backoff: {
          initialIntervalMs: RECONNECT_DELAY_MS,
          multiplier: 1.5,
          maxRetries: 10,
        },
      });

      await client.connect();

      // Create bidirectional stream
      const streamResult = await client.subscribe();
      stream = streamResult;

      if (!stream) {
        throw new Error("Failed to create gRPC stream");
      }

      stream.on("data", (data: any) => {
        handleStreamData(data);
      });

      stream.on("error", (err: any) => {
        console.error("[gRPC] Stream error:", err?.message ?? err);
        send({ type: "error", message: err?.message ?? "Stream error" });
        scheduleReconnect();
      });

      stream.on("end", () => {
        console.log("[gRPC] Stream ended");
        send({ type: "disconnected" });
        if (!isShuttingDown) scheduleReconnect();
      });

      stream.on("close", () => {
        console.log("[gRPC] Stream closed");
        if (!isShuttingDown) scheduleReconnect();
      });

      // Send subscription request
      const request = buildSubscribeRequest();
      stream.write(request);

      console.log("[gRPC] Subscribed to Pump.fun transactions");
      send({ type: "connected" });
      reconnectAttempts = 0;

      // Start ping to keep connection alive
      pingInterval = setInterval(() => {
        if (stream && !stream.destroyed) {
          try {
            stream.write({ ping: { id: Date.now() } });
          } catch {}
        }
      }, PING_INTERVAL_MS);

    } catch (err: any) {
      console.error("[gRPC] Connection failed:", err?.message ?? err);
      send({ type: "error", message: err?.message ?? "Connection failed" });
      scheduleReconnect();
    }
  }

  function handleStreamData(data: any) {
    try {
      // Handle pong responses
      if (data?.pong) return;

      // Handle transaction updates
      const txUpdate = data?.transactions?.[0];
      if (!txUpdate) return;

      const tx = txUpdate.transaction;
      const slot = txUpdate.slot ?? 0;

      if (!tx) return;

      // Parse trade event
      const event = parseTradeEvent(tx, Number(slot));
      if (!event) return;

      // Filter by target wallets
      const userLower = event.user.toLowerCase();
      if (!targetWalletSet.has(userLower)) return;

      // Validate required fields
      if (!event.mint || !event.signature) return;

      console.log(`[gRPC] Detected ${event.type.toUpperCase()}: ${event.mint} by ${event.user} (${event.solAmount / 1e9} SOL)`);

      send({ type: "trade", event });

    } catch (err: any) {
      console.error("[gRPC] Data handling error:", err?.message ?? err);
    }
  }

  function scheduleReconnect() {
    if (isShuttingDown) return;

    if (pingInterval) {
      clearInterval(pingInterval);
      pingInterval = null;
    }

    reconnectAttempts++;
    const delay = Math.min(RECONNECT_DELAY_MS * Math.pow(1.5, reconnectAttempts - 1), MAX_RECONNECT_DELAY_MS);

    console.log(`[gRPC] Reconnecting in ${delay}ms (attempt ${reconnectAttempts})`);
    send({ type: "reconnecting" });

    setTimeout(() => {
      if (!isShuttingDown) connectAndSubscribe();
    }, delay);
  }

  function shutdown() {
    isShuttingDown = true;
    if (pingInterval) clearInterval(pingInterval);
    if (stream) {
      try { stream.end(); } catch {}
    }
    if (client) {
      try { client.close(); } catch {}
    }
  }

  // Handle messages from main thread
  parentPort.on("message", (msg: any) => {
    if (msg?.type === "shutdown") {
      shutdown();
    } else if (msg?.type === "add_wallet") {
      targetWalletSet.add(msg.wallet.toLowerCase());
      // Update subscription with new wallet filter
      if (stream && !stream.destroyed) {
        const request = buildSubscribeRequest();
        stream.write(request);
      }
    } else if (msg?.type === "remove_wallet") {
      targetWalletSet.delete(msg.wallet.toLowerCase());
      if (stream && !stream.destroyed) {
        const request = buildSubscribeRequest();
        stream.write(request);
      }
    }
  });

  // Start connection
  connectAndSubscribe();

  // Handle process signals
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);

  send({ type: "ready" });
}

/* ================= MAIN THREAD: WORKER MANAGER ================= */

export class GrpcDetectionManager {
  private worker: Worker | null = null;
  private config: GrpcConfig;
  private onTradeCallback: ((event: GrpcTradeEvent) => void) | null = null;
  private onStatusChange: ((status: "connected" | "disconnected" | "reconnecting" | "error") => void) | null = null;
  private isRunning = false;

  constructor(config: GrpcConfig) {
    this.config = config;
  }

  setTradeHandler(handler: (event: GrpcTradeEvent) => void) {
    this.onTradeCallback = handler;
  }

  setStatusHandler(handler: (status: "connected" | "disconnected" | "reconnecting" | "error") => void) {
    this.onStatusChange = handler;
  }

  async start(): Promise<void> {
    if (this.isRunning) return;
    this.isRunning = true;

    return new Promise((resolve, reject) => {
      // Create worker thread
      this.worker = new Worker(__filename, { workerData: this.config });

      this.worker.on("message", (msg: WorkerMessage) => {
        switch (msg.type) {
          case "ready":
            console.log("✅ gRPC worker ready");
            resolve();
            break;
          case "trade":
            if (this.onTradeCallback && msg.event) {
              this.onTradeCallback(msg.event);
            }
            break;
          case "connected":
            this.onStatusChange?.("connected");
            break;
          case "disconnected":
            this.onStatusChange?.("disconnected");
            break;
          case "reconnecting":
            this.onStatusChange?.("reconnecting");
            break;
          case "error":
            console.error("[gRPC Manager] Error:", msg.message);
            this.onStatusChange?.("error");
            break;
        }
      });

      this.worker.on("error", (err) => {
        console.error("[gRPC Manager] Worker error:", err);
        this.onStatusChange?.("error");
        if (!this.worker) reject(err);
      });

      this.worker.on("exit", (code) => {
        console.log(`[gRPC Manager] Worker exited with code ${code}`);
        this.isRunning = false;
        this.onStatusChange?.("disconnected");
        if (code !== 0 && this.isRunning) {
          // Unexpected exit, could restart here
        }
      });

      // Timeout for ready
      setTimeout(() => {
        if (this.isRunning && !this.worker) {
          reject(new Error("gRPC worker failed to start"));
        }
      }, 10000);
    });
  }

  async stop(): Promise<void> {
    this.isRunning = false;
    if (this.worker) {
      this.worker.postMessage({ type: "shutdown" });
      // Wait for graceful shutdown
      await new Promise(resolve => setTimeout(resolve, 1000));
      this.worker.terminate();
      this.worker = null;
    }
  }

  addTargetWallet(wallet: string) {
    if (this.worker) {
      this.worker.postMessage({ type: "add_wallet", wallet });
    }
  }

  removeTargetWallet(wallet: string) {
    if (this.worker) {
      this.worker.postMessage({ type: "remove_wallet", wallet });
    }
  }

  getStatus(): boolean {
    return this.isRunning;
  }
}

/* ================= FACTORY FUNCTION ================= */

export function createGrpcDetectionManager(
  endpoint: string,
  token: string,
  targetWallets: string[],
  pumpFunProgramId: string = PUMP_FUN_PROGRAM_ID
): GrpcDetectionManager {
  return new GrpcDetectionManager({
    endpoint,
    token,
    targetWallets,
    pumpFunProgramId,
  });
}