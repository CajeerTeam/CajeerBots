-- CajeerBots 0.11.0: Job Scheduler & Maintenance product layer

CREATE TABLE IF NOT EXISTS "scheduled_jobs" (
  "id" TEXT NOT NULL,
  "code" TEXT NOT NULL,
  "title" TEXT NOT NULL,
  "type" TEXT NOT NULL DEFAULT 'built_in',
  "handler" TEXT,
  "schedule" TEXT NOT NULL DEFAULT 'manual',
  "timezone" TEXT NOT NULL DEFAULT 'UTC',
  "payload" JSONB,
  "isEnabled" BOOLEAN NOT NULL DEFAULT true,
  "maxAttempts" INTEGER NOT NULL DEFAULT 3,
  "timeoutSeconds" INTEGER NOT NULL DEFAULT 300,
  "lockUntil" TIMESTAMP(3),
  "lastRunAt" TIMESTAMP(3),
  "nextRunAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "scheduled_jobs_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX IF NOT EXISTS "scheduled_jobs_code_key" ON "scheduled_jobs"("code");
CREATE INDEX IF NOT EXISTS "scheduled_jobs_isEnabled_nextRunAt_idx" ON "scheduled_jobs"("isEnabled", "nextRunAt");
CREATE INDEX IF NOT EXISTS "scheduled_jobs_type_handler_idx" ON "scheduled_jobs"("type", "handler");

CREATE TABLE IF NOT EXISTS "scheduled_job_runs" (
  "id" TEXT NOT NULL,
  "jobId" TEXT NOT NULL,
  "status" TEXT NOT NULL DEFAULT 'running',
  "attempt" INTEGER NOT NULL DEFAULT 1,
  "triggeredBy" TEXT NOT NULL DEFAULT 'scheduler',
  "startedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "finishedAt" TIMESTAMP(3),
  "durationMs" INTEGER,
  "result" JSONB,
  "error" TEXT,
  "logs" JSONB,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "scheduled_job_runs_pkey" PRIMARY KEY ("id")
);

CREATE INDEX IF NOT EXISTS "scheduled_job_runs_jobId_createdAt_idx" ON "scheduled_job_runs"("jobId", "createdAt");
CREATE INDEX IF NOT EXISTS "scheduled_job_runs_status_createdAt_idx" ON "scheduled_job_runs"("status", "createdAt");
ALTER TABLE "scheduled_job_runs" ADD CONSTRAINT "scheduled_job_runs_jobId_fkey" FOREIGN KEY ("jobId") REFERENCES "scheduled_jobs"("id") ON DELETE CASCADE ON UPDATE CASCADE;

CREATE TABLE IF NOT EXISTS "maintenance_snapshots" (
  "id" TEXT NOT NULL,
  "kind" TEXT NOT NULL,
  "status" TEXT NOT NULL DEFAULT 'ok',
  "metrics" JSONB NOT NULL,
  "startedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "finishedAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "maintenance_snapshots_pkey" PRIMARY KEY ("id")
);

CREATE INDEX IF NOT EXISTS "maintenance_snapshots_kind_createdAt_idx" ON "maintenance_snapshots"("kind", "createdAt");
CREATE INDEX IF NOT EXISTS "maintenance_snapshots_status_createdAt_idx" ON "maintenance_snapshots"("status", "createdAt");

ALTER TABLE "bot_runtime_statuses" ALTER COLUMN "version" SET DEFAULT '0.11.0';
