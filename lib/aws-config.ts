import { S3Client } from "@aws-sdk/client-s3";

export function getBucketConfig() {
  const bucketName = process.env.AWS_BUCKET_NAME?.trim();
  if (!bucketName) {
    throw new Error("AWS_BUCKET_NAME is not configured.");
  }

  const rawPrefix = process.env.AWS_FOLDER_PREFIX?.trim() ?? "";
  const folderPrefix = rawPrefix && !rawPrefix.endsWith("/") ? `${rawPrefix}/` : rawPrefix;

  return {
    bucketName,
    folderPrefix,
  };
}

export function createS3Client() {
  return new S3Client({});
}
