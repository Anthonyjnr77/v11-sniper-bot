const fs = require('fs');
const code = fs.readFileSync('C:\\Users\\User\\Desktop\\Sniper-bot\\bot.ts', 'utf8');

let newCode = code.replace('import BN from \"bn.js\";\ntype BNType = typeof BN;', 'import BN from \"bn.js\";');
newCode = newCode.replace('type BNType = typeof BN;', '');
newCode = newCode.replace('virtualSolReserves: BNType;', 'virtualSolReserves: any;');
newCode = newCode.replace('virtualTokenReserves: BNType;', 'virtualTokenReserves: any;');
newCode = newCode.replace('realSolReserves: BNType;', 'realSolReserves: any;');
newCode = newCode.replace('realTokenReserves: BNType;', 'realTokenReserves: any;');

fs.writeFileSync('C:\\Users\\User\\Desktop\\Sniper-bot\\bot.ts', newCode);
console.log('Fixed BN type');