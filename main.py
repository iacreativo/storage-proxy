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
        
        # Configure Lifecycle Policy (3 days retention - 72 hours)
        config = LifecycleConfig(
            [
                Rule(
                    status="Enabled",
                    rule_id="DeleteOldPhotos",
                    expiration=Expiration(days=3),
                )
            ]
        )
        client.set_bucket_lifecycle(MINIO_BUCKET, config)
        print("Lifecycle policy (3 days) configured.")
        
        # Make bucket public for easy URL sharing
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
        client.set_bucket_policy(MINIO_BUCKET, json.dumps(policy))
        print("Bucket policy set to Public Read.")

    except Exception as e:
        print(f"Error during MinIO setup: {e}")

# Run setup on startup
setup_bucket()

@app.get("/health")
async def health():
    return {"status": "ok", "service": "storage-proxy"}

def optimize_image(content: bytes, max_width: int = 1200) -> bytes:
    """Resize and compress image to WebP Lossless."""
    try:
        img = Image.open(io.BytesIO(content))
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")
        if img.width > max_width:
            ratio = max_width / float(img.width)
            new_height = int(float(img.height) * ratio)
            img = img.resize((max_width, new_height), Image.Resampling.LANCZOS)
            
        out_io = io.BytesIO()
        # Using lossless=True for photographers high-fidelity
        img.save(out_io, format="WEBP", lossless=True, method=6)
        return out_io.getvalue()
    except Exception as e:
        print(f"Optimization error: {e}")
        return content

@app.post("/upload")
async def upload_file(
    file: UploadFile = File(None),
    image: str = Form(None),
    optimize: bool = Form(False)
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
            async with httpx.AsyncClient() as h_client:
                resp = await h_client.get(image, timeout=30)
                resp.raise_for_status()
                content = resp.content
                original_filename = image.split("/")[-1].split("?")[0] or "image.jpg"
                content_type = resp.headers.get("Content-Type", "image/jpeg")
        else:
            raise HTTPException(status_code=400, detail="No file or image URL provided")

        if optimize and content_type.startswith("image/"):
            content = optimize_image(content)
            original_filename = os.path.splitext(original_filename)[0] + ".webp"
            content_type = "image/webp"

        filename = f"{uuid.uuid4()}-{original_filename}"
        file_size = len(content)
        client.put_object(MINIO_BUCKET, filename, io.BytesIO(content), file_size, content_type=content_type)
        
        protocol = "https" if MINIO_SECURE else "http"
        file_url = f"{protocol}://{MINIO_ENDPOINT}/{MINIO_BUCKET}/{filename}"
        
        return {"status": "success", "data": {"url": file_url, "filename": filename, "size": file_size}}
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
        if e.code == "NoSuchKey": return []
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
    # Public gallery remains untouched (FIFO 30)
    try:
        gallery = []
        try:
            response = client.get_object(MINIO_BUCKET, "gallery.json")
            gallery = json.loads(response.read())
            response.close()
            response.release_conn()
        except S3Error as e:
            if e.code != "NoSuchKey": raise e

        # Auto-optimize for public view
        optimized_edited = item.edited_url
        async with httpx.AsyncClient() as h_client:
            try:
                resp = await h_client.get(item.edited_url, timeout=30)
                if resp.status_code == 200:
                    opt_content = optimize_image(resp.content, max_width=1200)
                    filename = f"gallery-{uuid.uuid4()}.webp"
                    client.put_object(MINIO_BUCKET, filename, io.BytesIO(opt_content), len(opt_content), content_type="image/webp")
                    protocol = "https" if MINIO_SECURE else "http"
                    optimized_edited = f"{protocol}://{MINIO_ENDPOINT}/{MINIO_BUCKET}/{filename}"
            except: pass

        new_entry = item.dict()
        new_entry["id"] = str(uuid.uuid4())
        new_entry["timestamp"] = time.time()
        new_entry["edited_url"] = optimized_edited
        gallery.insert(0, new_entry)
        if len(gallery) > 30: gallery.pop()

        gallery_data = json.dumps(gallery).encode("utf-8")
        client.put_object(MINIO_BUCKET, "gallery.json", io.BytesIO(gallery_data), len(gallery_data), content_type="application/json")
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# --- USER SPECIFIC LOGIC ---

class UserGalleryItem(BaseModel):
    user_id: str
    original_url: str
    edited_url: str
    preset_name: str
    preset_icon: str
    preset_description: str = ""

class UserErrorItem(BaseModel):
    user_id: str
    error_message: str
    execution_id: str = ""
    preset_name: str = "Unknown"

@app.post("/user-gallery")
async def update_user_gallery(item: UserGalleryItem):
    try:
        path = f"user_data/{item.user_id}/gallery.json"
        gallery = []
        try:
            response = client.get_object(MINIO_BUCKET, path)
            gallery = json.loads(response.read())
            response.close()
            response.release_conn()
        except S3Error as e:
            if e.code != "NoSuchKey": raise e

        # 1. Create Display Version (WebP Lossless)
        display_url = item.edited_url
        async with httpx.AsyncClient() as h_client:
            try:
                resp = await h_client.get(item.edited_url, timeout=30)
                if resp.status_code == 200:
                    opt_content = optimize_image(resp.content, max_width=1600)
                    filename = f"user_data/{item.user_id}/display-{uuid.uuid4()}.webp"
                    client.put_object(MINIO_BUCKET, filename, io.BytesIO(opt_content), len(opt_content), content_type="image/webp")
                    protocol = "https" if MINIO_SECURE else "http"
                    display_url = f"{protocol}://{MINIO_ENDPOINT}/{MINIO_BUCKET}/{filename}"
            except: pass

        new_entry = item.dict()
        new_entry["id"] = str(uuid.uuid4())
        new_entry["timestamp"] = time.time()
        new_entry["display_url"] = display_url
        new_entry["original_download_url"] = item.edited_url # The full resolution edit
        gallery.insert(0, new_entry)

        data = json.dumps(gallery).encode("utf-8")
        client.put_object(MINIO_BUCKET, path, io.BytesIO(data), len(data), content_type="application/json")
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/user-gallery")
async def get_user_gallery(user_id: str):
    try:
        path = f"user_data/{user_id}/gallery.json"
        response = client.get_object(MINIO_BUCKET, path)
        content = response.read()
        response.close()
        response.release_conn()
        return json.loads(content)
    except S3Error as e:
        if e.code == "NoSuchKey": return []
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/user-gallery/error")
async def log_user_error(item: UserErrorItem):
    try:
        path = f"user_data/{item.user_id}/errors.json"
        errors = []
        try:
            response = client.get_object(MINIO_BUCKET, path)
            errors = json.loads(response.read())
            response.close()
            response.release_conn()
        except S3Error as e:
            if e.code != "NoSuchKey": raise e

        new_error = item.dict()
        new_error["timestamp"] = time.time()
        errors.insert(0, new_error)
        
        # Keep only last 10 errors
        if len(errors) > 10: errors.pop()

        data = json.dumps(errors).encode("utf-8")
        client.put_object(MINIO_BUCKET, path, io.BytesIO(data), len(data), content_type="application/json")
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/user-status")
async def get_user_status(user_id: str):
    try:
        # Get Gallery Count
        gallery_path = f"user_data/{user_id}/gallery.json"
        count = 0
        try:
            response = client.get_object(MINIO_BUCKET, gallery_path)
            gallery = json.loads(response.read())
            count = len(gallery)
            response.close()
            response.release_conn()
        except: pass

        # Get Errors
        error_path = f"user_data/{user_id}/errors.json"
        errors = []
        try:
            response = client.get_object(MINIO_BUCKET, error_path)
            errors = json.loads(response.read())
            response.close()
            response.release_conn()
        except: pass

        return {
            "user_id": user_id,
            "images_generated": count,
            "recent_errors": errors,
            "expiry_hours": 72
        }
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
