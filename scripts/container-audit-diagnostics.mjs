// Diagnostics only: never change the scanner's exit code or expose secret match contents.
import { readFileSync, writeFileSync } from 'node:fs';
const [input, output] = process.argv.slice(2);
const report = JSON.parse(readFileSync(input, 'utf8'));
if (!Array.isArray(report.Results) || report.ArtifactType !== 'container_image') throw Error('Invalid container audit report');
const results = report.Results.map(r => ({
  Target: r.Target, Class: r.Class, Type: r.Type,
  Vulnerabilities: (r.Vulnerabilities ?? []).map(v => ({
    VulnerabilityID: v.VulnerabilityID, PkgName: v.PkgName, InstalledVersion: v.InstalledVersion,
    FixedVersion: v.FixedVersion, Severity: v.Severity, Status: v.Status, PrimaryURL: v.PrimaryURL,
  })),
  Secrets: (r.Secrets ?? []).map(s => ({ RuleID: s.RuleID, Severity: s.Severity })),
}));
const vulnerabilities = results.flatMap(r => r.Vulnerabilities);
const secrets = results.flatMap(r => r.Secrets);
const counts = Object.fromEntries(['HIGH','CRITICAL'].map(level => [level, vulnerabilities.filter(v => v.Severity === level).length]));
console.log('Container audit:', JSON.stringify({ image: report.Metadata?.ImageID, os: report.Metadata?.OS, vulnerabilities: counts, secretFindings: secrets.length }));
for (const r of results) for (const v of r.Vulnerabilities) console.log(JSON.stringify({ target: r.Target, ...v }));
writeFileSync(output, JSON.stringify({ ArtifactType: report.ArtifactType, ImageID: report.Metadata?.ImageID, OS: report.Metadata?.OS, Results: results }, null, 2) + '\n');
