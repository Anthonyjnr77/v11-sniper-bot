const fs = require('fs');
const file = 'C:\\Users\\User\\Desktop\\Sniper-bot\\bot.ts';
let code = fs.readFileSync(file, 'utf8');

const old = 'Check bonding curve cache first to avoid \"zero tokens\" on fresh tokens
      let cachedCurve = bondingCurveCache.get(mint);
      if (cachedCurve && Date.now() - cachedCurve.timestamp < BONDING_CURVE_TTL_MS) {
        console.log(\"âš¡ Using cached bonding curve for\", mint);
      } else {
        cachedCurve = null;
      }
      const [buyState';

const newCode = 'Check bonding curve cache first to avoid \"zero tokens\" on fresh tokens
      const cachedCurve = bondingCurveCache.get(mint) ?? null;
      if (cachedCurve && Date.now() - cachedCurve.timestamp < BONDING_CURVE_TTL_MS) {
        console.log(\"âš¡ Using cached bonding curve for\", mint);
      }
      // else cache miss or expired, use buyState.bondingCurve
      const [buyState';

if (code.includes(old)) {
    code = code.replace(old, newCode);
    fs.writeFileSync(file, code);
    console.log('Fixed cachedCurve type');
} else {
    console.log('Pattern not found');
}