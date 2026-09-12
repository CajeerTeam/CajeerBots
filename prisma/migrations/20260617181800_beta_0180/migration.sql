CREATE TABLE "release_freezes" (
  "id" TEXT NOT NULL,
  "version" TEXT NOT NULL,
  "channel" TEXT NOT NULL DEFAULT 'beta',
  "status" TEXT NOT NULL DEFAULT 'locked',
  "scope" JSONB NOT NULL DEFAULT '{}',
  "reason" TEXT,
  "lockedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "lockedBy" TEXT,
  "releasedAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "release_freezes_pkey" PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX "release_freezes_version_channel_key" ON "release_freezes"("version", "channel");

CREATE TABLE "release_quality_gates" (
  "id" TEXT NOT NULL,
  "code" TEXT NOT NULL,
  "title" TEXT NOT NULL,
  "category" TEXT NOT NULL,
  "severity" TEXT NOT NULL DEFAULT 'error',
  "isEnabled" BOOLEAN NOT NULL DEFAULT true,
  "config" JSONB NOT NULL DEFAULT '{}',
  "lastStatus" TEXT,
  "lastRunAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "release_quality_gates_pkey" PRIMARY KEY ("id")
);
CREATE UNIQUE INDEX "release_quality_gates_code_key" ON "release_quality_gates"("code");

CREATE TABLE "release_gate_runs" (
  "id" TEXT NOT NULL,
  "version" TEXT NOT NULL,
  "channel" TEXT NOT NULL DEFAULT 'beta',
  "status" TEXT NOT NULL,
  "rootDir" TEXT,
  "trigger" TEXT NOT NULL DEFAULT 'manual',
  "summary" JSONB NOT NULL DEFAULT '{}',
  "results" JSONB NOT NULL DEFAULT '[]',
  "startedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "finishedAt" TIMESTAMP(3),
  "durationMs" INTEGER,
  "createdBy" TEXT,
  "gateId" TEXT,
  CONSTRAINT "release_gate_runs_pkey" PRIMARY KEY ("id")
);
CREATE INDEX "release_gate_runs_version_channel_status_idx" ON "release_gate_runs"("version", "channel", "status");
CREATE INDEX "release_gate_runs_startedAt_idx" ON "release_gate_runs"("startedAt");
ALTER TABLE "release_gate_runs" ADD CONSTRAINT "release_gate_runs_gateId_fkey" FOREIGN KEY ("gateId") REFERENCES "release_quality_gates"("id") ON DELETE SET NULL ON UPDATE CASCADE;

CREATE TABLE "release_blockers" (
  "id" TEXT NOT NULL,
  "runId" TEXT,
  "code" TEXT NOT NULL,
  "severity" TEXT NOT NULL DEFAULT 'blocker',
  "status" TEXT NOT NULL DEFAULT 'open',
  "source" TEXT,
  "message" TEXT NOT NULL,
  "details" JSONB NOT NULL DEFAULT '{}',
  "acceptedReason" TEXT,
  "resolvedAt" TIMESTAMP(3),
  "resolvedBy" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "release_blockers_pkey" PRIMARY KEY ("id")
);
CREATE INDEX "release_blockers_status_severity_idx" ON "release_blockers"("status", "severity");
CREATE INDEX "release_blockers_runId_idx" ON "release_blockers"("runId");
ALTER TABLE "release_blockers" ADD CONSTRAINT "release_blockers_runId_fkey" FOREIGN KEY ("runId") REFERENCES "release_gate_runs"("id") ON DELETE SET NULL ON UPDATE CASCADE;

CREATE TABLE "release_candidate_reports" (
  "id" TEXT NOT NULL,
  "version" TEXT NOT NULL,
  "channel" TEXT NOT NULL DEFAULT 'beta',
  "status" TEXT NOT NULL,
  "gateRunId" TEXT,
  "summary" JSONB NOT NULL DEFAULT '{}',
  "markdown" TEXT,
  "artifactName" TEXT,
  "artifactSha256" TEXT,
  "createdBy" TEXT,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "release_candidate_reports_pkey" PRIMARY KEY ("id")
);
CREATE INDEX "release_candidate_reports_version_channel_status_idx" ON "release_candidate_reports"("version", "channel", "status");
ALTER TABLE "release_candidate_reports" ADD CONSTRAINT "release_candidate_reports_gateRunId_fkey" FOREIGN KEY ("gateRunId") REFERENCES "release_gate_runs"("id") ON DELETE SET NULL ON UPDATE CASCADE;
