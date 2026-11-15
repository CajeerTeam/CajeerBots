// Real PostgreSQL concurrency checks; only in disposable rehearsal.
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { startRuntimeStatus } from '../apps/bot-runtime/dist/runtime/runtime-records.js';
import { capacitySql, verifyCapacitySql } from './pilot-policy.mjs';
import { PrismaClient } from '../prisma/generated/client/index.js';
assert.equal(process.env.CAJEER_REHEARSAL_ISOLATED,'1');
const db = new PrismaClient();
try {
  await db.$transaction(async tx=>{
    await tx.$executeRawUnsafe('LOCK TABLE communities, bot_accounts IN SHARE ROW EXCLUSIVE MODE');
    await tx.pilotCapacity.create({data:{id:1,maxCommunities:2,maxBotAccounts:1,communities:await tx.community.count(),botAccounts:await tx.botAccount.count()}});
  });
  const results=await Promise.allSettled(['a','b'].map(s=>db.community.create({data:{slug:`pilot-${s}`,title:s}})));
  assert.equal(results.filter(r=>r.status==='fulfilled').length,1);
  const community=results.find(r=>r.status==='fulfilled').value;
  const bots=await Promise.allSettled(['a','b'].map(externalId=>db.botAccount.create({data:{communityId:community.id,provider:'telegram',externalId,isActive:false}})));
  assert.equal(bots.filter(r=>r.status==='fulfilled').length,1);
  await db.community.delete({where:{id:community.id}});
  const capacity=await db.pilotCapacity.findUniqueOrThrow({where:{id:1}});
  assert.equal(capacity.communities,1); assert.equal(capacity.botAccounts,0);
  assert.equal(await db.community.count(),1); assert.equal(await db.botAccount.count(),0);
  const c=await db.community.create({data:{slug:'pilot-reuse',title:'Reuse'}}); await db.community.delete({where:{id:c.id}});
  // Exercise the production SQL, including the ON CONFLICT path for an existing
  // 1.0.0 pilot. Prisma executes one SQL statement at a time inside its transaction.
  const configure = (communities,bots) => db.$transaction(async tx => {
    for(const sql of capacitySql(communities,bots).split(';').map(s=>s.trim()).filter(s=>s && s!=='BEGIN' && s!=='COMMIT')) await tx.$executeRawUnsafe(sql);
  });
  await configure(3,2);
  assert.equal((await db.pilotCapacity.findUniqueOrThrow({where:{id:1}})).maxCommunities,3);
  const extra=await db.community.create({data:{slug:'pilot-expanded',title:'Expanded'}});
  await assert.rejects(configure(1,2));
  assert.equal((await db.pilotCapacity.findUniqueOrThrow({where:{id:1}})).maxCommunities,3);
  await db.community.delete({where:{id:extra.id}});
  await configure(2,1);
  const [valid]=await db.$queryRawUnsafe(verifyCapacitySql);
  assert.equal(Object.values(valid)[0],'ok');
  await db.pilotCapacity.delete({where:{id:1}});
  const [invalid]=await db.$queryRawUnsafe(verifyCapacitySql);
  assert.equal(Object.values(invalid)[0],'invalid');
  await configure(2,1);
  const tenant=await db.community.create({data:{slug:'pilot-health-fixture',title:'Health fixture'}});
  const bot=await db.botAccount.create({data:{communityId:tenant.id,provider:'telegram',externalId:'pilot-health',mode:'polling',tokenRef:'env:TELEGRAM_BOT_TOKEN',isActive:true}});
  const account={...bot,token:'fixture-only'};
  const probe=()=>JSON.parse(execFileSync(process.execPath,['scripts/pilot-probe.mjs'],{encoding:'utf8',timeout:20000}));
  const oldStatus=await startRuntimeStatus(db,account);
  await oldStatus.beat();
  // Explicit timestamps make ordering deterministic even within one millisecond.
  await db.botRuntimeStatus.update({where:{id:oldStatus.id},data:{startedAt:new Date(Date.now()-1000)}});
  const current=await startRuntimeStatus(db,account);
  await current.beat(false);
  assert.equal(probe().healthyBots,0,'A historical running row must not hide the latest failed heartbeat');
  await current.beat(true);
  assert.equal(probe().healthyBots,1);
  await db.botAccount.update({where:{id:bot.id},data:{tokenRef:'env:TELEGRAM_BOT_TOKEN_SECOND'}});
  await current.beat();
  assert.equal((await db.botRuntimeStatus.findUniqueOrThrow({where:{id:current.id}})).status,'stopped');
  assert.equal(probe().healthyBots,0);
  await db.community.delete({where:{id:tenant.id}});
  await current.beat(); await current.stop(); // cascaded deletion is safe
  assert.equal(probe().passed,true);
  console.log('Pilot DB capacity: concurrency, rollback and cascading deletion verified');
} finally { await db.$disconnect(); }
