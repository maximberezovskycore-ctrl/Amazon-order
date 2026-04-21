import { NextRequest, NextResponse } from 'next/server';
import { saveLocalUpload } from '@/lib/s3';

export const dynamic = 'force-dynamic';

export async function POST(request: NextRequest) {
  try {
    const formData = await request.formData();
    const file = formData.get('file');

    if (!(file instanceof File)) {
      return NextResponse.json({ error: 'file is required' }, { status: 400 });
    }

    const buffer = Buffer.from(await file.arrayBuffer());
    const cloud_storage_path = await saveLocalUpload(file.name, buffer);

    return NextResponse.json({ cloud_storage_path });
  } catch (error: any) {
    console.error('Local upload error:', error);
    return NextResponse.json({ error: error?.message ?? 'Local upload failed' }, { status: 500 });
  }
}
