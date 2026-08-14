import 'dotenv/config';
import fetch from 'node-fetch';
import https from 'https';

const PING_URL = process.env.KEEPALIVE_URL ?? process.env.RENDER_URL ?? process.env.KEEPALIVE_ENDPOINT;
const INTERVAL_MS = Number(process.env.KEEPALIVE_INTERVAL_MS ?? 5 * 60 * 1000);

if (!PING_URL) {
  console.error('Missing KEEPALIVE_URL / RENDER_URL / KEEPALIVE_ENDPOINT env var');
  process.exit(1);
}

const agent = new https.Agent({ keepAlive: true });

console.log(`Keep-alive pinging ${PING_URL} every ${INTERVAL_MS / 1000}s`);

async function ping() {
  try {
    const res = await fetch(PING_URL, { agent });
    console.log(`PING ${PING_URL} -> ${res.status}`);
  } catch (e: any) {
    console.error('Keepalive ping failed:', e.message ?? e);
  }
}

ping();
setInterval(ping, INTERVAL_MS);
