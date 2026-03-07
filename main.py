import os, time, uuid, asyncio, json
from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from pydantic import BaseModel
import httpx, io
from PIL import Image
from fastapi.responses import JSONResponse
from minio import Minio
from minio.error import S3Error
from minio.commonconfig import CopySource
from minio.lifecycleconfig import LifecycleConfig, Rule, Expiration, Transition
from dotenv import load_dotenv

load_dotenv()

from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="SnapAI Storage Proxy")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Config from env
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio-backend.vyzo.cloud")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "snap-photos")
MINIO_SECURE = os.getenv("MINIO_SECURE", "True").lower() == "true"

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

def optimize_image(content: bytes, max_width: int = 1280) -> bytes:
    """Resize and compress image to WebP."""
    try:
        img = Image.open(io.BytesIO(content))
        
        # Convert to RGB if necessary (e.g. RGBA)
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")
            
        # Resize if width > max_width
        if img.width > max_width:
            ratio = max_width / float(img.width)
            new_height = int(float(img.height) * ratio)
            img = img.resize((max_width, new_height), Image.Resampling.LANCZOS)
            
        # Save to WebP
        out_io = io.BytesIO()
        img.save(out_io, format="WEBP", quality=80, method=6)
        return out_io.getvalue()
    except Exception as e:
        print(f"Optimization error: {e}")
        return content # Return original if optimization fails

@app.post("/upload")
async def upload_file(
    file: UploadFile = File(None),
    image: str = Form(None),
    optimize: bool = Form(False) # Inverted default to protect production quality
):
    try:
        content = b""
        original_filename = ""
        content_type = ""

        if file:
            content = await file.read()
            original_filename = file.filename
            content_type = file.content_type
        elif image:
            # Handle URL upload (mimic ImgBB)
            async with httpx.AsyncClient() as h_client:
                resp = await h_client.get(image, timeout=30)
                resp.raise_for_status()
                content = resp.content
                original_filename = image.split("/")[-1].split("?")[0] or "image.jpg"
                content_type = resp.headers.get("Content-Type", "image/jpeg")
        else:
            raise HTTPException(status_code=400, detail="No file or image URL provided")

        # Optimize image ONLY if requested (for gallery)
        if optimize and content_type.startswith("image/"):
            content = optimize_image(content)
            original_filename = os.path.splitext(original_filename)[0] + ".webp"
            content_type = "image/webp"

        filename = f"{uuid.uuid4()}-{original_filename}"
        file_size = len(content)
        
        data = io.BytesIO(content)
        
        client.put_object(
            MINIO_BUCKET,
            filename,
            data,
            file_size,
            content_type=content_type
        )
        
        # Construct URL
        protocol = "https" if MINIO_SECURE else "http"
        file_url = f"{protocol}://{MINIO_ENDPOINT}/{MINIO_BUCKET}/{filename}"
        
        return {
            "status": "success",
            "data": {
                "url": file_url,
                "filename": filename,
                "size": file_size
            }
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/gallery")
async def get_gallery():
    try:
        response = client.get_object(MINIO_BUCKET, "gallery.json")
        content = response.read()
        response.close()
        response.release_conn()
        return json.loads(content)
    except S3Error as e:
        if e.code == "NoSuchKey":
            return []
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class GalleryItem(BaseModel):
    original_url: str
    edited_url: str
    preset_name: str
    preset_icon: str
    preset_description: str = ""

@app.post("/gallery")
async def update_gallery(item: GalleryItem):
    try:
        # 1. Load current gallery
        gallery = []
        try:
            response = client.get_object(MINIO_BUCKET, "gallery.json")
            gallery = json.loads(response.read())
            response.close()
            response.release_conn()
        except S3Error as e:
            if e.code != "NoSuchKey":
                raise e

        # 2. Add new item
        new_entry = item.model_dump()
        new_entry["id"] = str(uuid.uuid4())
        new_entry["timestamp"] = time.time()
        gallery.insert(0, new_entry)

        # 3. FIFO Logic: Keep only 30
        if len(gallery) > 30:
            to_remove = gallery.pop()
            # Clean up files in MinIO
            for url_key in ["original_url", "edited_url"]:
                url = to_remove.get(url_key)
                if url and MINIO_ENDPOINT in url:
                    filename_to_del = url.split("/")[-1]
                    try:
                        client.remove_object(MINIO_BUCKET, filename_to_del)
                        print(f"Deleted old gallery file: {filename_to_del}")
                    except Exception as del_e:
                        print(f"Failed to delete {filename_to_del}: {del_e}")

        # 4. Save updated gallery.json
        gallery_data = json.dumps(gallery).encode("utf-8")
        from io import BytesIO
        client.put_object(
            MINIO_BUCKET,
            "gallery.json",
            BytesIO(gallery_data),
            len(gallery_data),
            content_type="application/json"
        )

        return {"status": "success", "count": len(gallery)}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/file/{filename}")
async def delete_file(filename: str):
    try:
        client.remove_object(MINIO_BUCKET, filename)
        return {"status": "success", "message": f"File {filename} deleted"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
