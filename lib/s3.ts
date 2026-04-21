import {
  PutObjectCommand,
  GetObjectCommand,
  DeleteObjectCommand,
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";
import { createClient } from "@supabase/supabase-js";
import { mkdir, readFile, writeFile } from "fs/promises";
import os from "os";
import path from "path";
import { createS3Client, getBucketConfig } from "./aws-config";

const s3 = createS3Client();
const LOCAL_STORAGE_DIR = process.env.LOCAL_STORAGE_DIR || path.join(os.tmpdir(), "amazon-sourcing-storage");
const SUPABASE_STORAGE_BUCKET = process.env.SUPABASE_STORAGE_BUCKET || "sourcing-files";

function safeFileName(fileName: string) {
  return fileName.replace(/[^A-Za-z0-9._-]+/g, "_").slice(0, 160) || "file";
}

export function isLocalStoragePath(cloud_storage_path: string) {
  return cloud_storage_path.startsWith("local://");
}

export function isSupabaseStoragePath(cloud_storage_path: string) {
  return cloud_storage_path.startsWith("supabase://");
}

function getSupabaseStorageClient() {
  const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL;
  const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

  if (!supabaseUrl || !serviceRoleKey) return null;

  return createClient(supabaseUrl, serviceRoleKey, {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
    },
  });
}

function supabasePath(cloud_storage_path: string) {
  return cloud_storage_path.replace(/^supabase:\/\//, "");
}

export async function saveUpload(fileName: string, buffer: Buffer, contentType = "application/octet-stream") {
  const supabase = getSupabaseStorageClient();

  if (!supabase) {
    return saveLocalUpload(fileName, buffer);
  }

  const storagePath = `uploads/${Date.now()}-${safeFileName(fileName)}`;
  const { error } = await supabase.storage
    .from(SUPABASE_STORAGE_BUCKET)
    .upload(storagePath, buffer, {
      contentType,
      upsert: false,
    });

  if (error) {
    throw new Error(`Supabase upload failed: ${error.message}`);
  }

  return `supabase://${storagePath}`;
}

export async function saveOutput(fileName: string, buffer: Buffer) {
  const supabase = getSupabaseStorageClient();

  if (!supabase) {
    return saveLocalOutput(fileName, buffer);
  }

  const storagePath = `outputs/${Date.now()}-${safeFileName(fileName)}`;
  const { error } = await supabase.storage
    .from(SUPABASE_STORAGE_BUCKET)
    .upload(storagePath, buffer, {
      contentType: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
      upsert: false,
    });

  if (error) {
    throw new Error(`Supabase output upload failed: ${error.message}`);
  }

  return {
    storedName: storagePath,
    downloadUrl: `/api/download/storage?file=${encodeURIComponent(storagePath)}`,
  };
}

export async function getSupabaseStorageBuffer(fileName: string) {
  const supabase = getSupabaseStorageClient();
  if (!supabase) throw new Error("Supabase Storage is not configured.");

  const { data, error } = await supabase.storage
    .from(SUPABASE_STORAGE_BUCKET)
    .download(fileName);

  if (error) {
    throw new Error(`Supabase download failed: ${error.message}`);
  }

  return Buffer.from(await data.arrayBuffer());
}

export async function saveLocalUpload(fileName: string, buffer: Buffer) {
  const storedName = `${Date.now()}-${safeFileName(fileName)}`;
  const uploadDir = path.join(LOCAL_STORAGE_DIR, "uploads");
  await mkdir(uploadDir, { recursive: true });
  await writeFile(path.join(uploadDir, storedName), buffer);
  return `local://uploads/${storedName}`;
}

export async function saveLocalOutput(fileName: string, buffer: Buffer) {
  const storedName = `${Date.now()}-${safeFileName(fileName)}`;
  const outputDir = path.join(LOCAL_STORAGE_DIR, "outputs");
  await mkdir(outputDir, { recursive: true });
  await writeFile(path.join(outputDir, storedName), buffer);
  return { storedName, downloadUrl: `/api/download/local?file=${encodeURIComponent(storedName)}` };
}

export async function getLocalOutputBuffer(fileName: string) {
  return readFile(path.join(LOCAL_STORAGE_DIR, "outputs", safeFileName(fileName)));
}

export async function generatePresignedUploadUrl(
  fileName: string,
  contentType: string,
  isPublic = false
) {
  const { bucketName, folderPrefix } = getBucketConfig();
  const prefix = isPublic ? `${folderPrefix}public/uploads` : `${folderPrefix}uploads`;
  const cloud_storage_path = `${prefix}/${Date.now()}-${fileName}`;
  const command = new PutObjectCommand({
    Bucket: bucketName,
    Key: cloud_storage_path,
    ContentType: contentType,
    ContentDisposition: isPublic ? "attachment" : undefined,
  });
  const uploadUrl = await getSignedUrl(s3, command, { expiresIn: 3600 });
  return { uploadUrl, cloud_storage_path };
}

export async function getFileUrl(cloud_storage_path: string, isPublic: boolean) {
  const { bucketName } = getBucketConfig();
  if (isPublic) {
    const region = process.env.AWS_REGION ?? "us-east-1";
    return `https://${bucketName}.s3.${region}.amazonaws.com/${cloud_storage_path}`;
  }
  const command = new GetObjectCommand({
    Bucket: bucketName,
    Key: cloud_storage_path,
    ResponseContentDisposition: "attachment",
  });
  return getSignedUrl(s3, command, { expiresIn: 3600 });
}

export async function deleteFile(cloud_storage_path: string) {
  const { bucketName } = getBucketConfig();
  const command = new DeleteObjectCommand({
    Bucket: bucketName,
    Key: cloud_storage_path,
  });
  await s3.send(command);
}

export async function getFileBuffer(cloud_storage_path: string): Promise<Buffer> {
  if (isLocalStoragePath(cloud_storage_path)) {
    const relativePath = cloud_storage_path.replace(/^local:\/\//, "");
    return readFile(path.join(LOCAL_STORAGE_DIR, relativePath));
  }

  if (isSupabaseStoragePath(cloud_storage_path)) {
    return getSupabaseStorageBuffer(supabasePath(cloud_storage_path));
  }

  const { bucketName } = getBucketConfig();
  const command = new GetObjectCommand({
    Bucket: bucketName,
    Key: cloud_storage_path,
  });
  const response = await s3.send(command);
  const stream = response.Body as any;
  const chunks: Buffer[] = [];
  for await (const chunk of stream) {
    chunks.push(Buffer.from(chunk));
  }
  return Buffer.concat(chunks);
}
