# Deploy to Render

## GitHub

Upload this folder as the app root:

`amazon_sourcing_app/nextjs_space`

Do not upload `.env`, `.env.local`, `.next`, `node_modules`, `.local-storage`, or log files.

## Render settings

Create a new Web Service from the GitHub repo.

Use these settings:

```text
Root Directory: amazon_sourcing_app/nextjs_space
Build Command: npm ci --no-audit --no-fund && npm run db:generate && npm run build
Start Command: npm run start
```

Add these environment variables in Render:

```text
NODE_ENV=production
USE_POSTGRES_JOBS=true
NEXT_PUBLIC_SUPABASE_URL=...
NEXT_PUBLIC_SUPABASE_PUBLISHABLE_KEY=...
SUPABASE_SERVICE_ROLE_KEY=...
SUPABASE_STORAGE_BUCKET=sourcing-files
DATABASE_URL=...
DIRECT_URL=...
```

Create a private Supabase Storage bucket named `sourcing-files`.

## Database

Run the migration once after adding `DATABASE_URL` and `DIRECT_URL`:

```bash
npm run db:migrate
```

On Render this can be run from the service shell, or locally before deploy if your database is reachable.

## Important

Local upload storage is only for development. On Render, use Supabase Storage with `SUPABASE_SERVICE_ROLE_KEY` and `SUPABASE_STORAGE_BUCKET`.
