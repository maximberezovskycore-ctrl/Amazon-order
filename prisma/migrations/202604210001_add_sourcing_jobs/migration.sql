CREATE TABLE "SourcingJob" (
    "id" TEXT NOT NULL,
    "status" TEXT NOT NULL,
    "progress" TEXT NOT NULL,
    "result" JSONB,
    "error" TEXT,
    "createdAt" TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP,
    "updatedAt" TIMESTAMP(3) NOT NULL,

    CONSTRAINT "SourcingJob_pkey" PRIMARY KEY ("id")
);

CREATE INDEX "SourcingJob_createdAt_idx" ON "SourcingJob"("createdAt");
CREATE INDEX "SourcingJob_status_idx" ON "SourcingJob"("status");
