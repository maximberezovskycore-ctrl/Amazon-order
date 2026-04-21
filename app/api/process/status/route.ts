import { NextRequest, NextResponse } from 'next/server';
import { getJob } from '@/lib/job-store';

export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest) {
  const jobId = request.nextUrl.searchParams.get('jobId');
  if (!jobId) {
    return NextResponse.json({ error: 'Missing jobId parameter' }, { status: 400 });
  }

  const job = await getJob(jobId);
  if (!job) {
    return NextResponse.json({ error: 'Job not found or expired' }, { status: 404 });
  }

  if (job.status === 'completed' && job.result) {
    return NextResponse.json({
      status: 'completed',
      progress: job.progress,
      ...job.result,
    });
  }

  if (job.status === 'error') {
    return NextResponse.json({
      status: 'error',
      error: job.error ?? 'Processing failed',
      progress: job.progress,
    });
  }

  return NextResponse.json({
    status: job.status,
    progress: job.progress,
  });
}
