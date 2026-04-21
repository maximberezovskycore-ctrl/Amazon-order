import { NextResponse } from 'next/server';

export const dynamic = 'force-dynamic';

export async function GET() {
  const ExcelJS = (await import('exceljs')).default;
  const workbook = new ExcelJS.Workbook();
  const worksheet = workbook.addWorksheet('Supplier Template');
  const columns = ['UPC', 'Cost', 'MPN', 'Vendor'];

  worksheet.columns = columns.map((name) => ({
    header: name,
    key: name,
    width: Math.max(name.length + 8, 18),
  }));

  worksheet.addRow({
    UPC: '012345678905',
    Cost: 12.5,
    MPN: 'ABC-123',
    Vendor: 'Example Vendor',
  });

  const header = worksheet.getRow(1);
  header.eachCell((cell) => {
    cell.font = { bold: true, color: { argb: 'FFFFFFFF' } };
    cell.fill = { type: 'pattern', pattern: 'solid', fgColor: { argb: 'FF111827' } };
    cell.alignment = { horizontal: 'center' };
  });

  const buffer = Buffer.from(await workbook.xlsx.writeBuffer());

  return new NextResponse(new Uint8Array(buffer), {
    headers: {
      'Content-Type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
      'Content-Disposition': 'attachment; filename="supplier-template.xlsx"',
      'Cache-Control': 'no-store',
    },
  });
}
