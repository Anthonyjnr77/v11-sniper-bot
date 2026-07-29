const fs = require('fs');
const code = fs.readFileSync('C:\\Users\\User\\Desktop\\Sniper-bot\\bot.ts', 'utf8');

let newCode = code.replace('import BN from \"bn.js\";\n', 'import BN from \"bn.js\";');
newCode = newCode.replace('virtualSolReserves: any;', 'virtualSolReserves: BN;');
newCode = newCode.replace('virtualTokenReserves: any;', 'virtualTokenReserves: BN;');
newCode = newCode.replace('realSolReserves: any;', 'realSolReserves: BN;');
newCode = newCode.replace('realTokenReserves: any;', 'realTokenReserves: BN;');

fs.writeFileSync('C:\\Users\\User\\Desktop\\Sniper-bot\\bot.ts', newCode);
console.log('Fixed BN');