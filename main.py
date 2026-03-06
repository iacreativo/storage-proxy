import os, time, uuid, asyncio
from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from minio import Minio
from minio.error import S3Error
from minio.commonconfig import CopySource
from minio.lifecycleconfig import LifecycleConfig, Rule, Expiration, Transition
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="SnapAI Storage Proxy")

# Config from env
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "q0wgw4scg8s8o4cg4884wc4g.178.156.248.186.sslip.io")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "snap-photos")
MINIO_SECURE = os.getenv("MINIO_SECURE", "False").lower() == "true"

# Initialize MinIO client
client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=MINIO_SECURE
)

def setup_bucket():
    try:
        if not client.bucket_exists(MINIO_BUCKET):
            client.make_bucket(MINIO_BUCKET)
            print(f"Bucket {MINIO_BUCKET} created.")
        
        # Configure Lifecycle Policy (2 days retention)
        config = LifecycleConfig(
            [
                Rule(
                    status="Enabled",
                    rule_id="DeleteOldPhotos",
                    expiration=Expiration(days=2),
                )
            ]
        )
        client.set_bucket_lifecycle(MINIO_BUCKET, config)
        print("Lifecycle policy (2 days) configured.")
        
        # Make bucket public for easy URL sharing (as requested for ImgBB replacement)
        # In a real production environment, we might want signed URLs, but for now 
        # mimicking ImgBB behavior is easier.
        policy = {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": ["*"]},
                    "Action": ["s3:GetBucketLocation", "s3:ListBucket"],
                    "Resource": [f"arn:aws:s3:::{MINIO_BUCKET}"]
                },
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": ["*"]},
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{MINIO_BUCKET}/*"]
                }
            ]
        }
        import json
        client.set_bucket_policy(MINIO_BUCKET, json.dumps(policy))
        print("Bucket policy set to Public Read.")

    except S3Error as e:
        print(f"Error during MinIO setup: {e}")

# Run setup on startup
setup_bucket()

@app.get("/health")
async def health():
    return {"status": "ok", "service": "storage-proxy"}

@app.post("/upload")
async def upload_file(file: UploadFile = File(...)):
    filename = f"{uuid.uuid4()}-{file.filename}"
    try:
        # Read file into memory or stream to MinIO
        content = await file.read()
        file_size = len(content)
        
        from io import BytesIO
        data = BytesIO(content)
        
        client.put_object(
            MINIO_BUCKET,
            filename,
            data,
            file_size,
            content_type=file.content_type
        )
        
        # Construct URL
        # For public read, the URL is http://endpoint/bucket/filename
        protocol = "https" if MINIO_SECURE else "http"
        file_url = f"{protocol}://{MINIO_ENDPOINT}/{MINIO_BUCKET}/{filename}"
        
        return {
            "status": "success",
            "url": file_url,
            "filename": filename,
            "size": file_size
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
