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
DATABASE_URL=...
DIRECT_URL=...
```

For file uploads in production, also add storage variables if using S3-compatible storage:

```text
AWS_REGION=...
AWS_BUCKET_NAME=...
AWS_FOLDER_PREFIX=...
```

## Database

Run the migration once after adding `DATABASE_URL` and `DIRECT_URL`:

```bash
npm run db:migrate
```

On Render this can be run from the service shell, or locally before deploy if your database is reachable.

## Important

Local upload storage (`.local-storage`) is only for development. On a hosted server, files can disappear after restart unless you use S3, Cloudflare R2, or another persistent storage service.
