const fs = require('fs');
const file = 'C:\\Users\\User\\Desktop\\Sniper-bot\\bot.ts';
let code = fs.readFileSync(file, 'utf8');

// Fix the bondingCurve type - buyState.bondingCurve could be PublicKey or BondingCurve
code = code.replace('bondingCurve: cachedCurve?.bondingCurve ?? buyState.bondingCurve,', 'bondingCurve: (cachedCurve?.bondingCurve ?? (typeof buyState.bondingCurve === \"string\" ? new PublicKey(buyState.bondingCurve) : buyState.bondingCurve)),');

fs.writeFileSync(file, code);
console.log('Fixed bondingCurve type');