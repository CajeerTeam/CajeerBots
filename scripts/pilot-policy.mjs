export function pilotLimits(communities, bots) {
  const maxCommunities = Number(communities), maxBotAccounts = Number(bots);
  if (!/^[1-9]\d*$/.test(String(communities)) || !/^[1-9]\d*$/.test(String(bots)) || !Number.isInteger(maxCommunities) || maxCommunities > 5 || !Number.isInteger(maxBotAccounts) || maxBotAccounts > 10) throw Error('Pilot limits: 1–5 communities, 1–10 bot accounts');
  return { maxCommunities, maxBotAccounts };
}
export function capacitySql(communities, bots) {
  const { maxCommunities, maxBotAccounts } = pilotLimits(communities,bots);
  // Reconfigure an existing 1.0.0 pilot atomically; CHECK constraints reject limits
  // below actual usage. Hold the same locks as the insert/delete accounting triggers.
  return `BEGIN; LOCK TABLE communities, bot_accounts IN SHARE ROW EXCLUSIVE MODE; INSERT INTO pilot_capacity (id,"maxCommunities","maxBotAccounts",communities,"botAccounts") SELECT 1,${maxCommunities},${maxBotAccounts},(SELECT count(*) FROM communities),(SELECT count(*) FROM bot_accounts) ON CONFLICT (id) DO UPDATE SET "maxCommunities"=EXCLUDED."maxCommunities", "maxBotAccounts"=EXCLUDED."maxBotAccounts", communities=EXCLUDED.communities, "botAccounts"=EXCLUDED."botAccounts"; COMMIT;`;
}
const count = value => Number.isSafeInteger(value) && value >= 0;
export function evaluatePilot(snapshot, { maxBacklogAgeMs = 300000, maxBacklog = 1000, maxDead = 0 } = {}) {
  if (![maxBacklogAgeMs,maxBacklog,maxDead].every(count)) throw Error('Invalid pilot thresholds');
  const failures = [];
  if (!snapshot || typeof snapshot !== 'object') return {passed:false,failures:['snapshot']};
  if (snapshot.version !== '1.0.2' || !snapshot.health || Object.values(snapshot.health).some(v=>v!==true) || !['api','runtime','worker'].every(k=>snapshot.health[k]===true)) failures.push('health');
  const c = snapshot.capacity;
  if (!c || c.communities !== snapshot.communities || c.botAccounts !== snapshot.botAccounts || ![c.communities,c.botAccounts,c.maxCommunities,c.maxBotAccounts].every(count) || c.maxCommunities < 1 || c.maxCommunities > 5 || c.maxBotAccounts < 1 || c.maxBotAccounts > 10 || c.communities > c.maxCommunities || c.botAccounts > c.maxBotAccounts) failures.push('capacity');
  if (!count(snapshot.backlog) || snapshot.backlog > maxBacklog) failures.push('backlog');
  if (!Number.isFinite(snapshot.oldestDueMs) || snapshot.oldestDueMs < 0 || snapshot.oldestDueMs > maxBacklogAgeMs) failures.push('backlog-age');
  if (!count(snapshot.dead) || snapshot.dead > maxDead) failures.push('dead-letter');
  if (![snapshot.activeBots,snapshot.healthyBots,snapshot.botAccounts].every(count) || snapshot.activeBots > snapshot.botAccounts || snapshot.activeBots !== snapshot.healthyBots) failures.push('bot-heartbeat');
  return { passed: failures.length === 0, failures };
}
// Capacity is checked before resuming any writer. A missing or inconsistent row
// must not be silently recreated or repaired by a resume operation.
export const verifyCapacitySql = `SELECT CASE WHEN EXISTS (SELECT 1 FROM pilot_capacity WHERE id=1 AND "maxCommunities" BETWEEN 1 AND 5 AND "maxBotAccounts" BETWEEN 1 AND 10 AND communities=(SELECT count(*) FROM communities) AND "botAccounts"=(SELECT count(*) FROM bot_accounts) AND communities BETWEEN 0 AND "maxCommunities" AND "botAccounts" BETWEEN 0 AND "maxBotAccounts") THEN 'ok' ELSE 'invalid' END`;

// Providers can briefly be disconnected while their clients finish reconnecting.
// Fail closed on capacity/dead-letter errors and never waive a final failed gate.
export async function waitForPilot(read, { attempts = 12, pause = () => new Promise(resolve => setTimeout(resolve, 5000)) } = {}) {
  if (!Number.isSafeInteger(attempts) || attempts < 1 || attempts > 12) throw Error('Invalid pilot readiness attempts');
  let result;
  for (let attempt = 0; attempt < attempts; attempt++) {
    const snapshot = await read();
    result = {...snapshot,...evaluatePilot(snapshot)};
    if (result.passed || result.failures.some(f => !['health','bot-heartbeat','backlog','backlog-age'].includes(f)) || attempt === attempts-1) return result;
    await pause();
  }
  return result;
}
