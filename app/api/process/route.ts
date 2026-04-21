import { NextRequest, NextResponse } from 'next/server';
import { getFileBuffer, saveOutput } from '@/lib/s3';
import { processFiles, generateExcel, isKeepaFile, parseCSVExport, type RunConfig, DEFAULT_CONFIG } from '@/lib/sourcing-engine';
import { PutObjectCommand } from '@aws-sdk/client-s3';
import { createS3Client, getBucketConfig } from '@/lib/aws-config';
import { getSignedUrl } from '@aws-sdk/s3-request-presigner';
import { GetObjectCommand } from '@aws-sdk/client-s3';
import * as XLSX from 'xlsx';
import { createJob, updateJob } from '@/lib/job-store';

export const maxDuration = 300;

export async function POST(request: NextRequest) {
  try {
    const body = await request.json();
    const { files, config: userConfig } = body ?? {};

    if (!files || !Array.isArray(files) || files.length === 0) {
      return NextResponse.json({ error: 'No files provided' }, { status: 400 });
    }

    // Create a job and return immediately
    const job = await createJob();
    const jobId = job.id;

    // Start background processing (fire-and-forget)
    runProcessing(jobId, files, userConfig).catch((err) => {
      console.error(`[Job ${jobId}] Unhandled error:`, err);
      updateJob(jobId, { status: 'error', error: err?.message ?? 'Processing failed' }).catch((updateErr) => {
        console.error(`[Job ${jobId}] Failed to persist error state:`, updateErr);
      });
    });

    return NextResponse.json({ jobId });
  } catch (error: any) {
    console.error('Processing error:', error);
    return NextResponse.json({ error: error?.message ?? 'Processing failed' }, { status: 500 });
  }
}

async function runProcessing(jobId: string, files: any[], userConfig: any) {
  const config: RunConfig = { ...DEFAULT_CONFIG, ...(userConfig ?? {}) };

  await updateJob(jobId, { status: 'processing', progress: 'Downloading files from storage...' });

  // Download all files from S3 and classify
  const supplierBuffers: { name: string; buffer: Buffer }[] = [];
  const keepaBuffers: { name: string; buffer: Buffer }[] = [];

  for (const file of files) {
    const { cloud_storage_path, fileName, fileType } = file ?? {};
    if (!cloud_storage_path || !fileName) continue;

    let buffer: Buffer;
    try {
      buffer = await getFileBuffer(cloud_storage_path);
    } catch (error: any) {
      throw new Error(`Uploaded file is no longer available: ${fileName}. Please upload the files again and start processing right away.`);
    }

    if (fileType === 'keepa') {
      keepaBuffers.push({ name: fileName, buffer });
    } else if (fileType === 'supplier') {
      supplierBuffers.push({ name: fileName, buffer });
    } else {
      // Auto-detect
      try {
        let columns: string[] = [];
        if (fileName.toLowerCase().endsWith('.csv')) {
          const data = parseCSVExport(buffer);
          if (data.length > 0) columns = Object.keys(data[0] ?? {});
        } else {
          const wb = XLSX.read(buffer, { type: 'buffer' });
          const ws = wb.Sheets[wb.SheetNames[0]];
          if (ws) {
            const data = XLSX.utils.sheet_to_json(ws, { defval: '' }) as Record<string, any>[];
            if (data.length > 0) columns = Object.keys(data[0] ?? {});
          }
        }
        if (isKeepaFile(columns)) {
          keepaBuffers.push({ name: fileName, buffer });
        } else {
          supplierBuffers.push({ name: fileName, buffer });
        }
      } catch {
        supplierBuffers.push({ name: fileName, buffer });
      }
    }
  }

  if (supplierBuffers.length === 0) {
    await updateJob(jobId, { status: 'error', error: 'No supplier file detected. Please upload at least one supplier file.' });
    return;
  }
  if (keepaBuffers.length === 0) {
    await updateJob(jobId, { status: 'error', error: 'No Keepa file detected. Please upload at least one Keepa export file.' });
    return;
  }

  await updateJob(jobId, { progress: 'Running 17-stage sourcing pipeline...' });

  // Process
  const result = processFiles(supplierBuffers, keepaBuffers, config);

  await updateJob(jobId, { progress: 'Generating Excel report...' });

  // Generate Excel
  const excelBuffer = await generateExcel(result);

  await updateJob(jobId, { progress: 'Uploading results to cloud storage...' });

  const today = new Date().toISOString().slice(0, 10);
  const vendorSafe = (result.vendor ?? 'UNKNOWN').replace(/[^A-Za-z0-9._-]+/g, '_').slice(0, 80) || 'UNKNOWN';
  const excelFileName = `Sourcing_Order_${vendorSafe}_${today}.xlsx`;

  const downloadUrl = await uploadResult(excelFileName, excelBuffer);

  await updateJob(jobId, {
    status: 'completed',
    progress: 'Done!',
    result: {
      summary: {
        ready: result.ready?.length ?? 0,
        borderline: result.borderline?.length ?? 0,
        removed: result.removed?.length ?? 0,
        unmatched: result.unmatched?.length ?? 0,
        confidence: result.confidence ?? 'LOW',
        vendor: result.vendor ?? 'UNKNOWN',
        warnings: result.warnings ?? [],
      },
      readyRows: (result.ready ?? []).slice(0, 50).map(r => ({
        ASIN: r['ASIN'],
        UPC: r['UPC'],
        Description: (r['Description'] ?? '').slice(0, 80),
        'Cost $': r['Cost $'],
        'BuyBox $': r['BuyBox $'],
        'Profit $': r['Profit $'],
        'ROI %': r['ROI %'],
        'Drops 90d': r['Drops 90d'],
        Qty: r['Qty'],
        Confidence: r['Confidence'],
      })),
      removedBreakdown: getRemovedBreakdown(result.removed ?? []),
      downloadUrl,
      fileName: excelFileName,
    },
  });
}

async function uploadResult(excelFileName: string, excelBuffer: Buffer) {
  try {
    const s3 = createS3Client();
    const { bucketName, folderPrefix } = getBucketConfig();
    const excelKey = `${folderPrefix}outputs/${Date.now()}-${excelFileName}`;

    await s3.send(new PutObjectCommand({
      Bucket: bucketName,
      Key: excelKey,
      Body: excelBuffer,
      ContentType: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      ContentDisposition: `attachment; filename="${excelFileName}"`,
    }));

    return getSignedUrl(s3, new GetObjectCommand({
      Bucket: bucketName,
      Key: excelKey,
      ResponseContentDisposition: `attachment; filename="${excelFileName}"`,
    }), { expiresIn: 3600 });
  } catch (error) {
    console.warn('Falling back to local output storage:', error);
    const local = await saveOutput(excelFileName, excelBuffer);
    return local.downloadUrl;
  }
}

function getRemovedBreakdown(removed: Record<string, any>[]): Record<string, number> {
  const breakdown: Record<string, number> = {};
  for (const r of removed) {
    const reason = String(r?.['Reason'] ?? 'Unknown').split(' | ')[0];
    breakdown[reason] = (breakdown[reason] ?? 0) + 1;
  }
  return breakdown;
}
