const fs = require('fs');
const file = 'C:\\Users\\User\\Desktop\\Sniper-bot\\bot.ts';
const code = fs.readFileSync(file, 'utf8');

const marker1 = 'async function executeBuy(';
const idx = code.indexOf(marker1);
if (idx === -1) {
  console.log('ERROR: executeBuy not found');
  process.exit(1);
}

// Find the exact text with garbled emoji
const replaceEndMarker = 'console.log("âš¨ Buy aborted';  
const replaceEndIdx = code.indexOf(replaceEndMarker, idx);
if (replaceEndIdx === -1) {
  console.log('ERROR: replaceEndMarker not found');
  process.exit(1);
}

// Find the closing brace of the if block
let braceCount = 0;
let found = false;
let endPos = replaceEndIdx;
for (let i = replaceEndIdx; i < code.length; i++) {
  if (code[i] === '{') braceCount++;
  if (code[i] === '}') {
    braceCount--;
    if (braceCount === 0) {
      endPos = i + 1;
      found = true;
      break;
    }
  }
}
if (!found) {
  console.log('ERROR: Could not find matching brace');
  process.exit(1);
}

const newCode = 'async function executeBuy(\n    mint: string,\n    targetSnapshotPromise?: Promise<{ tokenPriceUSD: number | null; marketCapUSD: number | null }>\n  ) {\n    if (botPaused) {\n      console.log(\"⏸️ Buy skipped — bot is paused:\", mint);\n      return;\n    }\n\n    const requiredLamports = BUY_AMOUNT + JITO_TIP_LAMPORTS + 3_000_000;\n    if (lastKnownBalanceLamports !== null && lastKnownBalanceLamports < requiredLamports) {\n      console.log(\n        ⏸️ Buy skipped — wallet  SOL below required ~ SOL:,\n        mint\n      );\n      return;\n    }\n\n    if (positions.has(mint) || inFlight.has(mint)) return;\n    inFlight.add(mint);\n\n    try {\n      let sig: string | null = null;\n\n      // ===== PHASE 2: PARALLEL BUILDER RACE =====\n      // Fire all builders simultaneously; first valid signed tx wins.\n      // This eliminates the sequential wait that was adding 300-800ms latency.\n      async function buildAndSend(builderName: string, buildFn: () => Promise<Uint8Array | null>) {\n        try {\n          const tx = await buildFn();\n          if (tx) {\n            const s = await sendRawTransactionDual(tx);\n            if (s) {\n              console.log(🚀 BUY submitted ():, mint, s);\n              return s;\n            }\n          }\n        } catch (e: any) {\n          console.log(⚠️  build error:, e.message);\n        }\n        return null;\n      }\n\n      const [sigLocal, sigPumpPortal, sigJupiter] = await Promise.allSettled([\n        buildAndSend(\"local Pump SDK\", () => buildLocalPumpBuyTx(mint)),\n        buildAndSend(\"PumpPortal-builder\", () => buildPumpPortalTx(\"buy\", mint, BUY_AMOUNT / 1e9, true, SLIPPAGE_BPS, \"pump\")),\n        buildAndSend(\"Jupiter\", () => submitJupiterBuy(mint)),\n      ]);\n\n      sig = sigLocal.value ?? sigPumpPortal.value ?? sigJupiter.value ?? null;\n\n      if (!sig) {\n        console.log(\"❌ Buy aborted — all builders failed for\", mint);\n        return;\n      }';

const oldCode = code.substring(idx, endPos);
code = code.replace(oldCode, newCode);

fs.writeFileSync(file, code);
console.log('✅ Replaced executeBuy with parallel builder race');