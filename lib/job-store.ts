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

  for (const [id, job] of memoryJobs) {
    if (job.createdAt < cutoff.getTime()) memoryJobs.delete(id);
  }

  if (hasDatabase()) {
    try {
      await prisma.sourcingJob.deleteMany({
        where: {
          createdAt: {
            lt: cutoff,
          },
        },
      });
    } catch (error) {
      console.warn('Job cleanup skipped because database is unavailable:', error);
    }
  }
}

function createMemoryJob(): JobState {
  const job: JobState = {
    id: `job_${Date.now()}_${randomUUID().slice(0, 8)}`,
    status: 'pending',
    progress: 'Queued...',
    createdAt: Date.now(),
  };
  memoryJobs.set(job.id, job);
  return job;
}

function rememberJob(job: JobState) {
  memoryJobs.set(job.id, job);
  return job;
}

function updateMemoryJob(id: string, updates: Partial<Omit<JobState, 'id' | 'createdAt'>>) {
  const job = memoryJobs.get(id);
  if (job) {
    Object.assign(job, updates);
  }
}

function databaseUpdates(updates: Partial<Omit<JobState, 'id' | 'createdAt'>>) {
  const data: {
    status?: JobStatus;
    progress?: string;
    result?: Prisma.InputJsonValue;
    error?: string | null;
  } = {};

  if (updates.status !== undefined) data.status = updates.status;
  if (updates.progress !== undefined) data.progress = updates.progress;
  if (updates.result !== undefined) data.result = updates.result as unknown as Prisma.InputJsonValue;
  if (updates.error !== undefined) data.error = updates.error;

  return data;
}

export async function createJob(): Promise<JobState> {
  await cleanupExpiredJobs();

  if (!hasDatabase()) {
    return createMemoryJob();
  }

  const id = `job_${Date.now()}_${randomUUID().slice(0, 8)}`;

  try {
    const job = await prisma.sourcingJob.create({
      data: {
        id,
        status: 'pending',
        progress: 'Queued...',
      },
    });

    return rememberJob(toJobState(job));
  } catch (error) {
    console.warn('Falling back to in-memory job store because database is unavailable:', error);
    const job: JobState = {
      id,
      status: 'pending',
      progress: 'Queued...',
      createdAt: Date.now(),
    };
    memoryJobs.set(id, job);
    return job;
  }
}

export async function getJob(id: string): Promise<JobState | undefined> {
  if (!hasDatabase()) {
    return memoryJobs.get(id);
  }

  try {
    const job = await prisma.sourcingJob.findUnique({
      where: { id },
    });

    return job ? rememberJob(toJobState(job)) : memoryJobs.get(id);
  } catch (error) {
    console.warn('Reading job from memory because database is unavailable:', error);
    return memoryJobs.get(id);
  }
}

export async function updateJob(id: string, updates: Partial<Omit<JobState, 'id' | 'createdAt'>>) {
  updateMemoryJob(id, updates);

  if (!hasDatabase()) {
    return;
  }

  try {
    await prisma.sourcingJob.update({
      where: { id },
      data: databaseUpdates(updates),
    });
  } catch (error) {
    console.warn('Job update kept in memory because database is unavailable:', error);
  }
}
