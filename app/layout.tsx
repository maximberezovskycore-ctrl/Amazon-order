import type { Metadata } from 'next';
import type { ReactNode } from 'react';
import './globals.css';

export const dynamic = 'force-dynamic';

export async function generateMetadata(): Promise<Metadata> {
  const base = process.env.NEXTAUTH_URL || 'http://localhost:3000';
  return {
    metadataBase: new URL(base),
    title: 'Amazon Wholesale Sourcing',
    description: 'Process supplier price files with Keepa data to generate ready-to-order sourcing templates.',
    icons: {
      icon: '/favicon.svg',
      shortcut: '/favicon.svg',
    },
    openGraph: {
      title: 'Amazon Wholesale Sourcing',
      description: 'Process supplier price files with Keepa data to generate ready-to-order sourcing templates.',
      images: ['/og-image.png'],
    },
  };
}

export default function RootLayout({ children }: { children: ReactNode }) {
  return (
    <html lang="en">
      <body className="min-h-screen antialiased">{children}</body>
    </html>
  );
}
