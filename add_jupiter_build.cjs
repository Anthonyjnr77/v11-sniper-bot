const fs = require('fs');
const file = 'C:\\Users\\User\\Desktop\\Sniper-bot\\bot.ts';
let code = fs.readFileSync(file, 'utf8');

const old = 'async function submitJupiterBuy(mint: string): Promise<string | null> {';
const newCode = '// Build Jupiter buy transaction (returns signed Uint8Array for parallel race)\nasync function buildJupiterBuyTx(mint) {\n  const quote = await getQuote(SOL_MINT, mint, BUY_AMOUNT);\n  if (!quote || !passesFilters(quote)) return null;\n  const txBase64 = await buildSwapTx(quote);\n  if (!txBase64) return null;\n  const swapTx = VersionedTransaction.deserialize(Buffer.from(txBase64, \"base64\"));\n  swapTx.sign([wallet]);\n  return swapTx.serialize();\n}\n\nasync function submitJupiterBuy(mint) {';

if (code.includes(old)) {
    code = code.replace(old, newCode);
    fs.writeFileSync(file, code);
    console.log('Added buildJupiterBuyTx');
} else {
    console.log('Pattern not found');
}