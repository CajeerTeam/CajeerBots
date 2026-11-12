export function pilotLimits(communities, bots) {
  const maxCommunities = Number(communities), maxBotAccounts = Number(bots);
  if (!/^[1-9]\d*$/.test(String(communities)) || !/^[1-9]\d*$/.test(String(bots)) || !Number.isInteger(maxCommunities) || maxCommunities > 5 || !Number.isInteger(maxBotAccounts) || maxBotAccounts > 10) throw Error('Pilot limits: 1–5 communities, 1–10 bot accounts');
  return { maxCommunities, maxBotAccounts };
}
export function capacitySql(communities, bots) {
  const { maxCommunities, maxBotAccounts } = pilotLimits(communities,bots);
  return `BEGIN; LOCK TABLE communities, bot_accounts IN SHARE ROW EXCLUSIVE MODE; INSERT INTO pilot_capacity (id,"maxCommunities","maxBotAccounts",communities,"botAccounts") SELECT 1,${maxCommunities},${maxBotAccounts},(SELECT count(*) FROM communities),(SELECT count(*) FROM bot_accounts); COMMIT;`;
}
export function evaluatePilot(snapshot, { maxBacklogAgeMs = 300000, maxBacklog = 1000, maxDead = 0 } = {}) {
  if (![maxBacklogAgeMs,maxBacklog,maxDead].every(v=>Number.isSafeInteger(v)&&v>=0)) throw Error('Invalid pilot thresholds');
  const failures = [];
  if (snapshot.version !== '1.0.0' || !snapshot.health || Object.values(snapshot.health).some(v=>v!==true) || !['api','runtime','worker'].every(k=>snapshot.health[k]===true)) failures.push('health');
  const c = snapshot.capacity;
  if (!c || c.communities !== snapshot.communities || c.botAccounts !== snapshot.botAccounts || ![c.communities,c.botAccounts,c.maxCommunities,c.maxBotAccounts].every(Number.isSafeInteger) || c.maxCommunities < 1 || c.maxCommunities > 5 || c.maxBotAccounts < 1 || c.maxBotAccounts > 10 || c.communities < 0 || c.botAccounts < 0 || c.communities > c.maxCommunities || c.botAccounts > c.maxBotAccounts) failures.push('capacity');
  if (!Number.isSafeInteger(snapshot.backlog) || snapshot.backlog > maxBacklog) failures.push('backlog');
  if (!Number.isFinite(snapshot.oldestDueMs) || snapshot.oldestDueMs > maxBacklogAgeMs) failures.push('backlog-age');
  if (!Number.isSafeInteger(snapshot.dead) || snapshot.dead > maxDead) failures.push('dead-letter');
  if (!Number.isSafeInteger(snapshot.activeBots) || snapshot.activeBots !== snapshot.healthyBots) failures.push('bot-heartbeat');
  return { passed: failures.length === 0, failures };
}
