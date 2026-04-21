'use client';

import type { ReactNode } from 'react';
import { useCallback, useRef, useState } from 'react';
import {
  AlertTriangle,
  Ban,
  CheckCircle2,
  ChevronDown,
  ChevronUp,
  Download,
  FileSpreadsheet,
  Loader2,
  Play,
  Settings2,
  Trash2,
  Upload,
  XCircle,
} from 'lucide-react';

interface UploadedFile {
  id: string;
  name: string;
  size: number;
  type: 'supplier' | 'keepa' | 'auto';
  cloud_storage_path: string;
}

interface Summary {
  ready: number;
  borderline: number;
  removed: number;
  unmatched: number;
  confidence: string;
  vendor: string;
  warnings: string[];
}

interface ProcessResult {
  summary: Summary;
  readyRows: Record<string, any>[];
  removedBreakdown: Record<string, number>;
  downloadUrl: string;
  fileName: string;
}

const DEFAULT_FILTERS = {
  min_roi_ready: 13,
  priority_roi: 15,
  borderline_min: 10,
  min_drops_90d: 5,
  max_offers_total: 20,
  max_fba_offers: 12,
  max_amazon_bb_pct: 30,
  max_bb_stddev: 25,
  max_bb_winners: 15,
  max_trend_drop: -40,
  new_listing_days: 14,
  min_rating: 0,
  min_review_count: 0,
};

const FILTER_FIELDS = [
  { key: 'priority_roi', label: 'Priority ROI %', step: 0.5 },
  { key: 'min_roi_ready', label: 'Ready ROI %', step: 0.5 },
  { key: 'borderline_min', label: 'Borderline ROI %', step: 0.5 },
  { key: 'min_drops_90d', label: 'Min Drops 90d', step: 1 },
  { key: 'max_offers_total', label: 'Max Offers', step: 1 },
  { key: 'max_fba_offers', label: 'Max FBA Offers', step: 1 },
  { key: 'max_amazon_bb_pct', label: 'Max Amazon BB %', step: 1 },
  { key: 'max_bb_stddev', label: 'Max BB StdDev %', step: 1 },
  { key: 'max_bb_winners', label: 'Max BB Winners', step: 1 },
  { key: 'max_trend_drop', label: 'Min Trend %', step: 1 },
  { key: 'new_listing_days', label: 'New Listing Days', step: 1 },
  { key: 'min_rating', label: 'Min Rating', step: 0.1 },
  { key: 'min_review_count', label: 'Min Reviews', step: 1 },
] as const;

export default function SourcingApp() {
  const [files, setFiles] = useState<UploadedFile[]>([]);
  const [filters, setFilters] = useState(DEFAULT_FILTERS);
  const [showFilters, setShowFilters] = useState(false);
  const [processing, setProcessing] = useState(false);
  const [progress, setProgress] = useState('');
  const [result, setResult] = useState<ProcessResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [uploading, setUploading] = useState(false);
  const [showWarnings, setShowWarnings] = useState(false);
  const [showBreakdown, setShowBreakdown] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

  const uploadLocalFile = useCallback(async (file: File): Promise<UploadedFile> => {
    const formData = new FormData();
    formData.append('file', file);

    const res = await fetch('/api/upload/local', {
      method: 'POST',
      body: formData,
    });
    const data = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(data?.error ?? `Could not upload ${file.name} locally`);

    return {
      id: crypto.randomUUID(),
      name: file.name,
      size: file.size,
      type: 'auto',
      cloud_storage_path: data.cloud_storage_path,
    };
  }, []);

  const uploadFile = useCallback(async (file: File): Promise<UploadedFile> => {
    return uploadLocalFile(file);
  }, [uploadLocalFile]);

  const handleFiles = useCallback(async (fileList: FileList | File[]) => {
    const valid = Array.from(fileList).filter((file) => {
      const ext = file.name.toLowerCase().split('.').pop();
      return ['xlsx', 'xls', 'csv'].includes(ext ?? '');
    });

    if (valid.length === 0) {
      setError('Upload Excel (.xlsx, .xls) or CSV (.csv) files.');
      return;
    }

    setUploading(true);
    setError(null);
    try {
      const uploaded = await Promise.all(valid.map((file) => uploadFile(file)));
      setFiles((prev) => [...prev, ...uploaded]);
    } catch (uploadError: any) {
      setError(uploadError?.message ?? 'File upload failed.');
    } finally {
      setUploading(false);
    }
  }, [uploadFile]);

  const processFiles = useCallback(async () => {
    if (files.length === 0) return;
    setProcessing(true);
    setError(null);
    setResult(null);
    setProgress('Starting processing job...');

    try {
      const res = await fetch('/api/process', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          files: files.map((file) => ({
            cloud_storage_path: file.cloud_storage_path,
            fileName: file.name,
            fileType: file.type === 'auto' ? undefined : file.type,
          })),
          config: filters,
        }),
      });
      const submitData = await res.json().catch(() => ({}));
      if (!res.ok) throw new Error(submitData?.error ?? 'Failed to start processing.');

      const jobId = submitData?.jobId;
      if (!jobId) throw new Error('No job ID returned.');

      setProgress('Processing files...');
      for (let attempts = 0; attempts < 300; attempts++) {
        await new Promise((resolve) => setTimeout(resolve, 3000));
        const statusRes = await fetch(`/api/process/status?jobId=${encodeURIComponent(jobId)}`);
        const statusData = await statusRes.json().catch(() => ({}));
        if (!statusRes.ok) throw new Error(statusData?.error ?? 'Could not read job status.');

        if (statusData.progress) setProgress(statusData.progress);
        if (statusData.status === 'completed') {
          setResult({
            summary: statusData.summary,
            readyRows: statusData.readyRows,
            removedBreakdown: statusData.removedBreakdown,
            downloadUrl: statusData.downloadUrl,
            fileName: statusData.fileName,
          });
          setProgress('');
          return;
        }
        if (statusData.status === 'error') throw new Error(statusData.error ?? 'Processing failed.');
      }
      throw new Error('Processing timed out after 15 minutes. Try smaller files.');
    } catch (processError: any) {
      setError(processError?.message ?? 'Processing failed.');
      setProgress('');
    } finally {
      setProcessing(false);
    }
  }, [files, filters]);

  const downloadExcel = useCallback(() => {
    if (!result?.downloadUrl) return;
    const link = document.createElement('a');
    link.href = result.downloadUrl;
    link.download = result.fileName ?? 'sourcing-output.xlsx';
    link.rel = 'noopener noreferrer';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  }, [result]);

  const formatSize = (bytes: number) => {
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1048576) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / 1048576).toFixed(1)} MB`;
  };

  return (
    <div className="min-h-screen bg-background text-foreground">
      <header className="border-b border-border bg-card">
        <div className="mx-auto flex max-w-5xl flex-col gap-4 px-4 py-6 sm:flex-row sm:items-center sm:justify-between">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.18em] text-muted-foreground">Amazon sourcing</p>
            <h1 className="mt-2 text-2xl font-semibold tracking-tight">Wholesale Sourcing</h1>
            <p className="mt-1 max-w-2xl text-sm text-muted-foreground">
              Upload supplier and Keepa files, run filters, then download the finished order workbook.
            </p>
          </div>
          <a
            href="/api/template"
            className="inline-flex items-center justify-center gap-2 rounded-lg border border-border bg-background px-4 py-2 text-sm font-medium hover:bg-secondary"
          >
            <Download className="h-4 w-4" />
            Download supplier template
          </a>
        </div>
      </header>

      <main className="mx-auto max-w-5xl space-y-6 px-4 py-8">
        <section className="grid gap-4 md:grid-cols-3">
          <StepCard number="1" title="Supplier file" text="Use columns UPC, Cost, MPN, Vendor or download the template." />
          <StepCard number="2" title="Keepa export" text="Upload one or more Keepa exports with ASIN and Buy Box data." />
          <StepCard number="3" title="Result" text="Run processing and download the generated Excel workbook." />
        </section>

        <section className="rounded-2xl border border-border bg-card p-5 shadow-sm">
          <div className="mb-4 flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <div>
              <h2 className="text-lg font-semibold">Files</h2>
              <p className="text-sm text-muted-foreground">Supported formats: .xlsx, .xls, .csv</p>
            </div>
            <button
              type="button"
              onClick={() => setShowFilters((prev) => !prev)}
              className="inline-flex items-center justify-center gap-2 rounded-lg border border-border px-3 py-2 text-sm font-medium hover:bg-secondary"
            >
              <Settings2 className="h-4 w-4" />
              Filters
              {showFilters ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
            </button>
          </div>

          {showFilters && (
            <div className="mb-5 rounded-xl border border-border bg-background p-4">
              <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-4">
                {FILTER_FIELDS.map((field) => (
                  <label key={field.key} className="text-sm">
                    <span className="mb-1 block text-xs font-medium text-muted-foreground">{field.label}</span>
                    <input
                      type="number"
                      step={field.step}
                      value={filters[field.key]}
                      onChange={(event) => {
                        const value = Number.parseFloat(event.target.value);
                        setFilters((prev) => ({ ...prev, [field.key]: Number.isFinite(value) ? value : 0 }));
                      }}
                      className="w-full rounded-lg border border-input bg-card px-3 py-2 outline-none focus:ring-2 focus:ring-ring"
                    />
                  </label>
                ))}
              </div>
              <button
                type="button"
                onClick={() => setFilters(DEFAULT_FILTERS)}
                className="mt-3 text-sm font-medium text-primary hover:underline"
              >
                Reset filters
              </button>
            </div>
          )}

          <div
            onDrop={(event) => {
              event.preventDefault();
              if (event.dataTransfer.files) handleFiles(event.dataTransfer.files);
            }}
            onDragOver={(event) => event.preventDefault()}
            onClick={() => fileInputRef.current?.click()}
            className="cursor-pointer rounded-xl border-2 border-dashed border-border bg-background p-8 text-center hover:border-primary"
          >
            <input
              ref={fileInputRef}
              type="file"
              multiple
              accept=".xlsx,.xls,.csv"
              onChange={(event) => {
                if (event.target.files) handleFiles(event.target.files);
                event.target.value = '';
              }}
              className="hidden"
            />
            {uploading ? (
              <div className="flex flex-col items-center gap-3 text-sm text-muted-foreground">
                <Loader2 className="h-8 w-8 animate-spin text-primary" />
                Uploading files...
              </div>
            ) : (
              <div className="flex flex-col items-center gap-3">
                <Upload className="h-8 w-8 text-muted-foreground" />
                <div>
                  <p className="font-medium">Drop files here or click to browse</p>
                  <p className="mt-1 text-sm text-muted-foreground">Set each file type manually if auto-detect is wrong.</p>
                </div>
              </div>
            )}
          </div>

          {files.length > 0 && (
            <div className="mt-5 space-y-3">
              {files.map((file) => (
                <div key={file.id} className="flex flex-col gap-3 rounded-xl border border-border p-3 sm:flex-row sm:items-center sm:justify-between">
                  <div className="flex min-w-0 items-center gap-3">
                    <FileSpreadsheet className="h-5 w-5 shrink-0 text-primary" />
                    <div className="min-w-0">
                      <p className="truncate text-sm font-medium">{file.name}</p>
                      <p className="text-xs text-muted-foreground">{formatSize(file.size)}</p>
                    </div>
                  </div>
                  <div className="flex items-center gap-2">
                    <select
                      value={file.type}
                      onChange={(event) => {
                        const type = event.target.value as UploadedFile['type'];
                        setFiles((prev) => prev.map((item) => item.id === file.id ? { ...item, type } : item));
                      }}
                      className="rounded-lg border border-input bg-card px-3 py-2 text-sm"
                    >
                      <option value="auto">Auto-detect</option>
                      <option value="supplier">Supplier</option>
                      <option value="keepa">Keepa</option>
                    </select>
                    <button
                      type="button"
                      onClick={() => setFiles((prev) => prev.filter((item) => item.id !== file.id))}
                      className="rounded-lg border border-border p-2 hover:bg-secondary"
                      aria-label={`Remove ${file.name}`}
                    >
                      <Trash2 className="h-4 w-4" />
                    </button>
                  </div>
                </div>
              ))}

              <button
                type="button"
                onClick={processFiles}
                disabled={processing}
                className="inline-flex w-full items-center justify-center gap-2 rounded-xl bg-primary px-5 py-3 text-sm font-semibold text-primary-foreground hover:opacity-90 disabled:cursor-not-allowed disabled:opacity-60"
              >
                {processing ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
                {processing ? 'Processing...' : 'Process files'}
              </button>
            </div>
          )}
        </section>

        {progress && (
          <StatusBox tone="neutral" icon={<Loader2 className="h-5 w-5 animate-spin" />} text={progress} />
        )}

        {error && (
          <StatusBox tone="error" icon={<XCircle className="h-5 w-5" />} text={error} />
        )}

        {result && (
          <section className="space-y-5 rounded-2xl border border-border bg-card p-5 shadow-sm">
            <div className="grid gap-3 sm:grid-cols-2 lg:grid-cols-5">
              <Metric icon={<CheckCircle2 className="h-5 w-5" />} label="Ready" value={result.summary.ready} />
              <Metric icon={<AlertTriangle className="h-5 w-5" />} label="Borderline" value={result.summary.borderline} />
              <Metric icon={<XCircle className="h-5 w-5" />} label="Removed" value={result.summary.removed} />
              <Metric icon={<Ban className="h-5 w-5" />} label="Unmatched" value={result.summary.unmatched} />
              <Metric label="Confidence" value={result.summary.confidence} />
            </div>

            {result.summary.vendor && (
              <p className="text-sm text-muted-foreground">Vendor: <span className="font-medium text-foreground">{result.summary.vendor}</span></p>
            )}

            {(result.summary.warnings?.length ?? 0) > 0 && (
              <Disclosure
                title={`${result.summary.warnings.length} warning(s)`}
                open={showWarnings}
                onToggle={() => setShowWarnings((prev) => !prev)}
              >
                <ul className="space-y-1 text-sm text-muted-foreground">
                  {result.summary.warnings.map((warning, index) => (
                    <li key={`${warning}-${index}`}>{warning}</li>
                  ))}
                </ul>
              </Disclosure>
            )}

            {Object.keys(result.removedBreakdown ?? {}).length > 0 && (
              <Disclosure
                title="Removed breakdown"
                open={showBreakdown}
                onToggle={() => setShowBreakdown((prev) => !prev)}
              >
                <div className="space-y-2 text-sm">
                  {Object.entries(result.removedBreakdown)
                    .sort((a, b) => b[1] - a[1])
                    .map(([reason, count]) => (
                      <div key={reason} className="flex justify-between gap-4">
                        <span className="text-muted-foreground">{reason}</span>
                        <span className="font-mono">{count}</span>
                      </div>
                    ))}
                </div>
              </Disclosure>
            )}

            {(result.readyRows?.length ?? 0) > 0 && (
              <div className="overflow-hidden rounded-xl border border-border">
                <div className="border-b border-border bg-background px-4 py-3 text-sm font-medium">
                  Ready preview
                </div>
                <div className="overflow-x-auto">
                  <table className="w-full text-left text-sm">
                    <thead className="bg-background text-xs uppercase text-muted-foreground">
                      <tr>
                        {['ASIN', 'Description', 'Cost $', 'BuyBox $', 'Profit $', 'ROI %', 'Drops 90d', 'Qty'].map((heading) => (
                          <th key={heading} className="whitespace-nowrap px-3 py-2">{heading}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {result.readyRows.map((row, index) => (
                        <tr key={`${row.ASIN}-${index}`} className="border-t border-border">
                          <td className="whitespace-nowrap px-3 py-2 font-mono text-xs">{row.ASIN ?? '-'}</td>
                          <td className="max-w-[260px] truncate px-3 py-2">{row.Description ?? '-'}</td>
                          <td className="whitespace-nowrap px-3 py-2">${row['Cost $']?.toFixed?.(2) ?? '-'}</td>
                          <td className="whitespace-nowrap px-3 py-2">${row['BuyBox $']?.toFixed?.(2) ?? '-'}</td>
                          <td className="whitespace-nowrap px-3 py-2">${row['Profit $']?.toFixed?.(2) ?? '-'}</td>
                          <td className="whitespace-nowrap px-3 py-2">{row['ROI %']?.toFixed?.(1) ?? '-'}%</td>
                          <td className="whitespace-nowrap px-3 py-2">{row['Drops 90d'] ?? '-'}</td>
                          <td className="whitespace-nowrap px-3 py-2">{row.Qty ?? '-'}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            <button
              type="button"
              onClick={downloadExcel}
              className="inline-flex w-full items-center justify-center gap-2 rounded-xl bg-foreground px-5 py-3 text-sm font-semibold text-background hover:opacity-90"
            >
              <Download className="h-4 w-4" />
              Download result
            </button>
          </section>
        )}
      </main>
    </div>
  );
}

function StepCard({ number, title, text }: { number: string; title: string; text: string }) {
  return (
    <div className="rounded-2xl border border-border bg-card p-4 shadow-sm">
      <div className="mb-3 flex h-8 w-8 items-center justify-center rounded-full bg-primary text-sm font-semibold text-primary-foreground">
        {number}
      </div>
      <h3 className="font-semibold">{title}</h3>
      <p className="mt-1 text-sm text-muted-foreground">{text}</p>
    </div>
  );
}

function Metric({ icon, label, value }: { icon?: ReactNode; label: string; value: number | string }) {
  return (
    <div className="rounded-xl border border-border bg-background p-4">
      <div className="mb-2 text-primary">{icon}</div>
      <p className="text-2xl font-semibold">{value}</p>
      <p className="text-xs uppercase tracking-wide text-muted-foreground">{label}</p>
    </div>
  );
}

function StatusBox({ icon, text, tone }: { icon: ReactNode; text: string; tone: 'neutral' | 'error' }) {
  return (
    <div className={`flex items-start gap-3 rounded-xl border p-4 text-sm ${
      tone === 'error' ? 'border-red-200 bg-red-50 text-red-800' : 'border-border bg-card text-muted-foreground'
    }`}>
      {icon}
      <p>{text}</p>
    </div>
  );
}

function Disclosure({
  children,
  onToggle,
  open,
  title,
}: {
  children: ReactNode;
  onToggle: () => void;
  open: boolean;
  title: string;
}) {
  return (
    <div className="rounded-xl border border-border">
      <button
        type="button"
        onClick={onToggle}
        className="flex w-full items-center justify-between gap-3 px-4 py-3 text-left text-sm font-medium"
      >
        {title}
        {open ? <ChevronUp className="h-4 w-4" /> : <ChevronDown className="h-4 w-4" />}
      </button>
      {open && <div className="border-t border-border px-4 py-3">{children}</div>}
    </div>
  );
}
