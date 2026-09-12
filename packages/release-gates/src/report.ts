import type { ReleaseCandidateSummary, ReleaseGateRunReport } from './types.js';

export function buildReleaseCandidateSummary(report: ReleaseGateRunReport, gateRunId?: string): ReleaseCandidateSummary {
  const blockers = report.results.flatMap((result) => result.issues).filter((issue) => issue.severity === 'blocker' || issue.severity === 'error');
  const highlights = [
    `Release ${report.version} (${report.channel}) status: ${report.status}`,
    `Gates: ${report.summary.passed}/${report.summary.total} passed, ${report.summary.failed} failed, ${report.summary.warning} warnings`,
    `Blockers: ${blockers.length}`,
  ];
  return { version: report.version, channel: report.channel, status: report.status, gateRunId, generatedAt: report.generatedAt, highlights, blockers };
}

export function renderMarkdownReport(report: ReleaseGateRunReport): string {
  const lines = [
    `# Release gate report ${report.version}`,
    '',
    `Channel: ${report.channel}`,
    `Status: ${report.status}`,
    `Generated at: ${report.generatedAt}`,
    '',
    '## Summary',
    '',
    `- Total: ${report.summary.total}`,
    `- Passed: ${report.summary.passed}`,
    `- Warnings: ${report.summary.warning}`,
    `- Failed: ${report.summary.failed}`,
    `- Blockers: ${report.summary.blockers}`,
    '',
    '## Gates',
    '',
  ];
  for (const gate of report.results) {
    lines.push(`### ${gate.code}: ${gate.status}`);
    lines.push('');
    lines.push(`Category: ${gate.category}; severity: ${gate.severity}; duration: ${gate.durationMs}ms`);
    lines.push('');
    if (gate.issues.length === 0) lines.push('Issues: none');
    else {
      lines.push('Issues:');
      for (const issue of gate.issues.slice(0, 50)) lines.push(`- [${issue.severity}] ${issue.source ? `${issue.source}: ` : ''}${issue.message}`);
    }
    lines.push('');
  }
  return lines.join('\n');
}
