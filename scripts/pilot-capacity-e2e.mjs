// Real PostgreSQL concurrency checks; only in disposable rehearsal.
import assert from 'node:assert/strict';
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
  console.log('Pilot DB capacity: concurrency, rollback and cascading deletion verified');
} finally { await db.$disconnect(); }
