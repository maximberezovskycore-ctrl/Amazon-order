import { NextRequest, NextResponse } from 'next/server';
import { getLocalOutputBuffer } from '@/lib/s3';

export const dynamic = 'force-dynamic';

export async function GET(request: NextRequest) {
  try {
    const file = request.nextUrl.searchParams.get('file');
    if (!file) {
      return NextResponse.json({ error: 'file parameter is required' }, { status: 400 });
    }

    const buffer = await getLocalOutputBuffer(file);

    return new NextResponse(new Uint8Array(buffer), {
      headers: {
        'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        'Content-Disposition': `attachment; filename="${file.replace(/^\d+-/, '')}"`,
        'Cache-Control': 'no-store',
      },
    });
  } catch (error: any) {
    console.error('Local download error:', error);
    return NextResponse.json({ error: error?.message ?? 'Local download failed' }, { status: 404 });
  }
}
