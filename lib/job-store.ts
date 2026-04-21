import { randomUUID } from 'crypto';
import type { Prisma } from '@prisma/client';
import { prisma } from '@/lib/db';

export type JobStatus = 'pending' | 'processing' | 'completed' | 'error';

export interface JobResult {
  summary: any;
  readyRows: any[];
  removedBreakdown: Record<string, number>;
  downloadUrl: string;
  fileName: string;
}

export interface JobState {
  id: string;
  status: JobStatus;
  progress: string;
  createdAt: number;
  result?: JobResult;
  error?: string;
}

const JOB_TTL_MS = 24 * 60 * 60 * 1000;
const memoryJobs = new Map<string, JobState>();

function hasDatabase() {
  return process.env.USE_POSTGRES_JOBS === 'true' && Boolean(process.env.DATABASE_URL?.trim());
}

function toJobState(job: {
  id: string;
  status: string;
  progress: string;
  createdAt: Date;
  result: unknown;
  error: string | null;
}): JobState {
  return {
    id: job.id,
    status: normalizeStatus(job.status),
    progress: job.progress,
    createdAt: job.createdAt.getTime(),
    result: job.result ? (job.result as JobResult) : undefined,
    error: job.error ?? undefined,
  };
}

function normalizeStatus(status: string): JobStatus {
  if (status === 'pending' || status === 'processing' || status === 'completed' || status === 'error') {
    return status;
  }
  return 'error';
}

async function cleanupExpiredJobs() {
  const cutoff = new Date(Date.now() - JOB_TTL_MS);
  if (!hasDatabase()) {
    for (const [id, job] of memoryJobs) {
      if (job.createdAt < cutoff.getTime()) memoryJobs.delete(id);
    }
    return;
  }

  await prisma.sourcingJob.deleteMany({
    where: {
      createdAt: {
        lt: cutoff,
      },
    },
  });
}

export async function createJob(): Promise<JobState> {
  await cleanupExpiredJobs();

  if (!hasDatabase()) {
    const job: JobState = {
      id: `job_${Date.now()}_${randomUUID().slice(0, 8)}`,
      status: 'pending',
      progress: 'Queued...',
      createdAt: Date.now(),
    };
    memoryJobs.set(job.id, job);
    return job;
  }

  const job = await prisma.sourcingJob.create({
    data: {
      id: `job_${Date.now()}_${randomUUID().slice(0, 8)}`,
      status: 'pending',
      progress: 'Queued...',
    },
  });

  return toJobState(job);
}

export async function getJob(id: string): Promise<JobState | undefined> {
  if (!hasDatabase()) {
    return memoryJobs.get(id);
  }

  const job = await prisma.sourcingJob.findUnique({
    where: { id },
  });

  return job ? toJobState(job) : undefined;
}

export async function updateJob(id: string, updates: Partial<Omit<JobState, 'id' | 'createdAt'>>) {
  if (!hasDatabase()) {
    const job = memoryJobs.get(id);
    if (job) {
      Object.assign(job, updates);
    }
    return;
  }

  await prisma.sourcingJob.update({
    where: { id },
    data: {
      status: updates.status,
      progress: updates.progress,
      result: updates.result ? (updates.result as unknown as Prisma.InputJsonValue) : undefined,
      error: updates.error,
    },
  });
}
