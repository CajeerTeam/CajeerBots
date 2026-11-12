// Inside the API container. Read-only DB and HTTP checks; never emit credentials or payloads.
import { PrismaClient } from '../prisma/generated/client/index.js';
import { evaluatePilot } from './pilot-policy.mjs';
const prisma = new PrismaClient();
const get = async (url, authenticated=false) => {
  try { const r = await fetch(url,{headers:authenticated?{'x-api-key':process.env.CAJEER_ADMIN_TOKEN}:{},signal:AbortSignal.timeout(5000),redirect:'error'}); return { ok:r.ok, body:r.ok?await r.json():null }; } catch { return {ok:false,body:null}; }
};
try {
  const [api,runtime,worker,system] = await Promise.all([get('http://127.0.0.1:3000/api/health'),get('http://bot-runtime:3101/runtime/health'),get('http://worker:3102/worker/health'),get('http://127.0.0.1:3000/api/system/report',true)]);
  const [row, capacity] = await prisma.$transaction(async tx => {
  const [row] = await tx.$queryRaw`
    SELECT (SELECT count(*)::int FROM communities) AS communities,
    (SELECT count(*)::int FROM bot_accounts) AS "botAccounts",
    (SELECT count(*)::int FROM bot_accounts WHERE "isActive") AS "activeBots",
    (SELECT count(*)::int FROM bot_accounts a WHERE a."isActive" AND EXISTS (SELECT 1 FROM bot_runtime_statuses s WHERE s."accountId"=a.id AND s.status='running' AND s."lastBeatAt"> now()-interval '60 seconds')) AS "healthyBots",
    (SELECT count(*)::int FROM event_dead_letters WHERE status='open') + (SELECT count(*)::int FROM bot_outbound_messages WHERE status='dead') + (SELECT count(*)::int FROM outbox_messages WHERE status='dead') + (SELECT count(*)::int FROM extension_runtime_events WHERE status='dead') AS dead,
    (SELECT count(*)::int FROM event_subscription_deliveries WHERE status IN ('pending','processing','failed')) + (SELECT count(*)::int FROM bot_outbound_messages WHERE status IN ('pending','sending','failed')) + (SELECT count(*)::int FROM outbox_messages WHERE status IN ('pending','processing','failed')) + (SELECT count(*)::int FROM extension_runtime_events WHERE status IN ('pending','processing','failed')) AS backlog,
    COALESCE((SELECT greatest(0,extract(epoch FROM now()-min(due))*1000)::float8 FROM (SELECT "availableAt" AS due FROM event_subscription_deliveries WHERE status IN ('pending','processing','failed') UNION ALL SELECT "availableAt" FROM bot_outbound_messages WHERE status IN ('pending','sending','failed') UNION ALL SELECT "availableAt" FROM outbox_messages WHERE status IN ('pending','processing','failed') UNION ALL SELECT "availableAt" FROM extension_runtime_events WHERE status IN ('pending','processing','failed')) q),0) AS "oldestDueMs"`;
  const capacity = await tx.pilotCapacity.findUnique({where:{id:1}});
  return [row,capacity];
  }, {isolationLevel:'RepeatableRead',timeout:10000});
  const snapshot = {...row, capacity, version:system.body?.version ?? null, health:{api:api.ok&&api.body?.status==='ok',runtime:runtime.ok,worker:worker.ok}};
  console.log(JSON.stringify({...snapshot,...evaluatePilot(snapshot), checkedAt:new Date().toISOString()}));
} finally { await prisma.$disconnect(); }
