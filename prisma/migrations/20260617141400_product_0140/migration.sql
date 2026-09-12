CREATE TABLE "observability_metric_samples" (
  "id" TEXT NOT NULL,
  "communityId" TEXT,
  "source" TEXT NOT NULL,
  "metric" TEXT NOT NULL,
  "value" DOUBLE PRECISION NOT NULL,
  "unit" TEXT,
  "labels" JSONB,
  "windowSeconds" INTEGER,
  "severity" TEXT NOT NULL DEFAULT 'info',
  "collectedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  CONSTRAINT "observability_metric_samples_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "observability_probes" (
  "id" TEXT NOT NULL,
  "code" TEXT NOT NULL,
  "title" TEXT NOT NULL,
  "type" TEXT NOT NULL DEFAULT 'database',
  "target" TEXT,
  "intervalSeconds" INTEGER NOT NULL DEFAULT 60,
  "timeoutMs" INTEGER NOT NULL DEFAULT 2000,
  "isEnabled" BOOLEAN NOT NULL DEFAULT true,
  "status" TEXT NOT NULL DEFAULT 'unknown',
  "latencyMs" INTEGER,
  "lastCheckedAt" TIMESTAMP(3),
  "nextCheckAt" TIMESTAMP(3),
  "lastError" TEXT,
  "metadata" JSONB,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "observability_probes_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "observability_alert_rules" (
  "id" TEXT NOT NULL,
  "code" TEXT NOT NULL,
  "title" TEXT NOT NULL,
  "metric" TEXT NOT NULL,
  "operator" TEXT NOT NULL DEFAULT 'gte',
  "threshold" DOUBLE PRECISION NOT NULL,
  "severity" TEXT NOT NULL DEFAULT 'warning',
  "durationSeconds" INTEGER NOT NULL DEFAULT 0,
  "cooldownSeconds" INTEGER NOT NULL DEFAULT 300,
  "labels" JSONB,
  "isEnabled" BOOLEAN NOT NULL DEFAULT true,
  "lastTriggeredAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "observability_alert_rules_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "observability_incidents" (
  "id" TEXT NOT NULL,
  "code" TEXT NOT NULL,
  "title" TEXT NOT NULL,
  "status" TEXT NOT NULL DEFAULT 'open',
  "severity" TEXT NOT NULL DEFAULT 'warning',
  "source" TEXT NOT NULL DEFAULT 'alert',
  "metric" TEXT,
  "currentValue" DOUBLE PRECISION,
  "metadata" JSONB,
  "startedAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "resolvedAt" TIMESTAMP(3),
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "updatedAt" TIMESTAMP(3) NOT NULL,
  CONSTRAINT "observability_incidents_pkey" PRIMARY KEY ("id")
);

CREATE TABLE "observability_alert_events" (
  "id" TEXT NOT NULL,
  "ruleId" TEXT,
  "incidentId" TEXT,
  "status" TEXT NOT NULL DEFAULT 'fired',
  "severity" TEXT NOT NULL,
  "metric" TEXT NOT NULL,
  "value" DOUBLE PRECISION NOT NULL,
  "threshold" DOUBLE PRECISION NOT NULL,
  "message" TEXT NOT NULL,
  "metadata" JSONB,
  "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
  "resolvedAt" TIMESTAMP(3),
  CONSTRAINT "observability_alert_events_pkey" PRIMARY KEY ("id")
);

CREATE UNIQUE INDEX "observability_probes_code_key" ON "observability_probes"("code");
CREATE UNIQUE INDEX "observability_alert_rules_code_key" ON "observability_alert_rules"("code");
CREATE UNIQUE INDEX "observability_incidents_code_key" ON "observability_incidents"("code");
CREATE INDEX "observability_metric_samples_metric_collectedAt_idx" ON "observability_metric_samples"("metric", "collectedAt");
CREATE INDEX "observability_metric_samples_source_collectedAt_idx" ON "observability_metric_samples"("source", "collectedAt");
CREATE INDEX "observability_metric_samples_communityId_collectedAt_idx" ON "observability_metric_samples"("communityId", "collectedAt");
CREATE INDEX "observability_metric_samples_severity_collectedAt_idx" ON "observability_metric_samples"("severity", "collectedAt");
CREATE INDEX "observability_probes_isEnabled_nextCheckAt_idx" ON "observability_probes"("isEnabled", "nextCheckAt");
CREATE INDEX "observability_probes_status_lastCheckedAt_idx" ON "observability_probes"("status", "lastCheckedAt");
CREATE INDEX "observability_alert_rules_metric_isEnabled_idx" ON "observability_alert_rules"("metric", "isEnabled");
CREATE INDEX "observability_alert_rules_severity_isEnabled_idx" ON "observability_alert_rules"("severity", "isEnabled");
CREATE INDEX "observability_incidents_status_severity_startedAt_idx" ON "observability_incidents"("status", "severity", "startedAt");
CREATE INDEX "observability_incidents_metric_startedAt_idx" ON "observability_incidents"("metric", "startedAt");
CREATE INDEX "observability_alert_events_ruleId_createdAt_idx" ON "observability_alert_events"("ruleId", "createdAt");
CREATE INDEX "observability_alert_events_incidentId_createdAt_idx" ON "observability_alert_events"("incidentId", "createdAt");
CREATE INDEX "observability_alert_events_status_severity_createdAt_idx" ON "observability_alert_events"("status", "severity", "createdAt");
CREATE INDEX "observability_alert_events_metric_createdAt_idx" ON "observability_alert_events"("metric", "createdAt");

ALTER TABLE "observability_metric_samples" ADD CONSTRAINT "observability_metric_samples_communityId_fkey" FOREIGN KEY ("communityId") REFERENCES "communities"("id") ON DELETE SET NULL ON UPDATE CASCADE;
ALTER TABLE "observability_alert_events" ADD CONSTRAINT "observability_alert_events_ruleId_fkey" FOREIGN KEY ("ruleId") REFERENCES "observability_alert_rules"("id") ON DELETE SET NULL ON UPDATE CASCADE;
ALTER TABLE "observability_alert_events" ADD CONSTRAINT "observability_alert_events_incidentId_fkey" FOREIGN KEY ("incidentId") REFERENCES "observability_incidents"("id") ON DELETE SET NULL ON UPDATE CASCADE;
