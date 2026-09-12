import fs from 'node:fs';
let src=fs.readFileSync('/mnt/data/v22_direction_lab.js','utf8');
src=src.replace('import { DurableObject } from "cloudflare:workers";','class DurableObject {}');
src += '\nexport {updateSecondBucket, aggregateBucketsCompleted, completedBaselineQuotes, signDirection};\n';
const mod=await import('data:text/javascript;base64,'+Buffer.from(src).toString('base64'));
const {updateSecondBucket,aggregateBucketsCompleted,completedBaselineQuotes,signDirection}=mod;
const series=new Map();
function tr(ms,price,quote,dir){return {receivedAt:ms,price,quote,aggressorDir:dir};}
// 30 completed 1s buckets. Event window is 125000..130000. Baseline immediately before it.
for(let i=0;i<30;i++){
  const ms=100000+i*1000+100;
  updateSecondBucket(series,tr(ms,100+i*0.01, i>=25?400:100, i>=25?1:(i%2?1:-1)));
}
const f5=aggregateBucketsCompleted(series,130000,5000);
if(f5.count!==5) throw new Error('f5 count');
if(f5.quote!==2000) throw new Error('f5 quote');
if(f5.imbalance<=0.99) throw new Error('imbalance');
const base=completedBaselineQuotes(series,125000);
if(base[0]!==500) throw new Error('baseline overlaps event window or wrong first bucket: '+base[0]);
if(base.slice(0,5).some(x=>x!==500)) throw new Error('baseline bucket math');
if(signDirection(0.1)!=='UP'||signDirection(-0.1)!=='DOWN'||signDirection(0)!=='FLAT') throw new Error('direction labels');
console.log(JSON.stringify({ok:true,f5,baselineFirst5:base.slice(0,5)},null,2));
