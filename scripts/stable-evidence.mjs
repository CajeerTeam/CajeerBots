// The stable release requires actual, version-bound rehearsal and security evidence.
import { readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
export const stableVersion = '1.0.0';
export const stablePrefix = `cajeerbots-${stableVersion}`;
export const stableSuffixes = ['.zip', '.image.tar.gz', '.sbom.cdx.json', '.audit.json', '.rehearsal.json', '.container-audit.json'];
const requiredPhases = ['clean-install-stable','pilot-capacity-enforcement','clean-install-rc6','rc6-community-backup-and-sentinels','worker-unavailable','http-load-durable-backlog','worker-restart','queue-drain','upgrade-rc6-to-stable','verify-upgraded-data','stable-community-backup-restore','stable-stop-worker','create-discord-fixture','reload-bot-runtime','signed-discord-form-e2e','stable-http-load','stable-restart-worker','stable-queue-drain','restore-original-into-fresh-project','start-restored-rc6','verify-restored-data','stable-instance-backup-restore'];
const imageId = value => typeof value === 'string' && /^sha256:[a-f0-9]{64}$/.test(value);
const check = (condition, message) => { if (!condition) throw Error(message); };
export function validateStableEvidence({ report, audit, sbom, containerAudit, commit }) {
  check(/^[a-f0-9]{40}$/.test(commit), 'Invalid commit');
  check(report?.format === 'cajeerbots.rehearsal.v1' && report.passed === true && report.commit === commit && report.baselineVersion === '1.0.0-rc.6' && report.targetVersion === stableVersion, 'Wrong or failed rehearsal');
  check(imageId(report.targetImageId) && imageId(report.baselineImageId) && report.targetImageId !== report.baselineImageId, 'Missing immutable rehearsal image IDs');
  check(Number.isInteger(report.count) && report.count >= 1000 && report.count <= 10000 && Number.isInteger(report.concurrency) && report.concurrency >= 8 && report.concurrency <= 64, 'Insufficient rehearsal workload');
  check(Array.isArray(report.phases) && report.phases.length === requiredPhases.length && requiredPhases.every((name,i) => report.phases[i]?.name === name && report.phases[i].passed === true && Number.isFinite(report.phases[i].durationMs)), 'Missing, failed or unordered rehearsal phase');
  check(report.phases.find(p=>p.name==='pilot-capacity-enforcement').result?.passed===true, 'Pilot capacity probe failed');
  for (const name of ['http-load-durable-backlog','stable-http-load']) {
    const load = report.phases.find(p => p.name === name).result?.load;
    check(load?.passed === true && load.failed === 0 && Array.isArray(load.errors) && load.errors.length === 0 && load.succeeded === report.count && load.rps >= 10 && load.p95Ms <= 1500, `Load gate failed: ${name}`);
  }
  for (const name of ['queue-drain','stable-queue-drain']) {
    const result = report.phases.find(p=>p.name===name).result;
    check(result?.count===report.count && Number.isFinite(result.drainMs) && result.drainMs>=0 && result.drainMs<=180000, `Queue drain gate failed: ${name}`);
  }
  const baseline=report.phases.find(p=>p.name==='queue-drain').result;
  for (const name of ['verify-upgraded-data','verify-restored-data']) {
    const result=report.phases.find(p=>p.name===name).result;
    check(typeof baseline.id==='string' && baseline.id.length>0 && result?.id===baseline.id && result.count===report.count && result.communityId===baseline.communityId, `Data verification mismatch: ${name}`);
  }
  const stable = report.phases.find(p=>p.name==='stable-queue-drain').result;
  const stableRestore = report.phases.find(p=>p.name==='stable-instance-backup-restore').result;
  check(typeof stable.id==='string' && stable.id.length>0 && stableRestore?.id===stable.id && stableRestore.count===report.count && stableRestore.communityId===stable.communityId, 'Stable instance restore data mismatch');
  const vulnerabilities = audit?.metadata?.vulnerabilities;
  check(vulnerabilities && Number.isInteger(vulnerabilities.high) && Number.isInteger(vulnerabilities.critical) && vulnerabilities.high === 0 && vulnerabilities.critical === 0 && !audit.error, 'npm audit missing or high/critical findings');
  check(sbom?.bomFormat === 'CycloneDX' && sbom.specVersion === '1.5' && sbom.metadata?.component?.version === stableVersion && sbom.components?.some(c => c.name === 'nodejs') && sbom.components?.some(c => c.purl?.startsWith('pkg:deb/')), 'Incomplete stable SBOM');
  check(containerAudit?.ArtifactType === 'container_image' && containerAudit.Metadata?.ImageID === report.targetImageId && Array.isArray(containerAudit.Results) && containerAudit.Results.some(r => r.Class === 'os-pkgs'), 'Missing container scan of tested image');
  check(containerAudit.Results.every(r => (r.Vulnerabilities ?? []).every(v => !['HIGH','CRITICAL'].includes(v.Severity))), 'Container audit has high/critical findings');
  return report.targetImageId;
}
export async function readStableEvidence(dir, commit) {
  const json = async suffix => JSON.parse(await readFile(join(dir,stablePrefix+suffix),'utf8'));
  return validateStableEvidence({ report: await json('.rehearsal.json'), audit: await json('.audit.json'), sbom: await json('.sbom.cdx.json'), containerAudit: await json('.container-audit.json'), commit });
}
export function requireStableManifest(manifest) {
  check(manifest.version === stableVersion && manifest.channel === 'stable', 'Stable channel/version required');
  check(manifest.files.length === stableSuffixes.length && stableSuffixes.every(s => manifest.files.some(f => f.path === stablePrefix+s)), 'Incomplete stable artifact set');
}
// Docker save config filename is content-addressed. Verify the archive before it is signed or loaded.
export function verifyImageArchive(path, expectedId, expectedCommit) {
  check(imageId(expectedId), 'Invalid image ID');
  const tar = (args, maxBuffer=16*1024*1024) => {
    const result = spawnSync('tar', args, {encoding:null,maxBuffer,timeout:120000});
    check(!result.error && result.status === 0, 'Cannot read runtime archive'); return result.stdout;
  };
  const manifest = JSON.parse(tar(['-xOzf',path,'manifest.json']).toString('utf8'));
  check(Array.isArray(manifest) && manifest.length === 1, 'Expected exactly one saved image');
  check(Array.isArray(manifest[0].RepoTags) && manifest[0].RepoTags.length === 1 && manifest[0].RepoTags[0] === 'cajeerbots:1.0.0', 'Unexpected runtime image tag');
  const config = manifest[0].Config;
  check(typeof config === 'string' && /^(?:blobs\/sha256\/)?[a-f0-9]{64}(?:\.json)?$/.test(config), 'Invalid image config path');
  const bytes = tar(['-xOzf',path,config]);
  check('sha256:'+createHash('sha256').update(bytes).digest('hex') === expectedId, 'Archive image differs from rehearsed image');
  const metadata = JSON.parse(bytes.toString('utf8'));
  check(metadata.config?.Labels?.['org.opencontainers.image.revision'] === expectedCommit, 'Runtime image source commit mismatch');
  return expectedId;
}
