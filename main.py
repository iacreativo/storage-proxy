import os, time, uuid, asyncio, json
from fastapi import FastAPI, UploadFile, File, HTTPException, Form
from pydantic import BaseModel
import httpx, io
from PIL import Image
from fastapi.responses import JSONResponse, StreamingResponse
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

# Expiry configuration (in hours)
# Set to 72 for production, or lower value (e.g., 0.083 for 5 min) for testing
IMAGE_EXPIRY_HOURS = float(os.getenv("IMAGE_EXPIRY_HOURS", "72"))  # Default 72 hours (production)

# Supabase Config for Refunds
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "")

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
        
        # Configure Lifecycle Policy (5 minutes retention for testing)
        # Using 1 day as MinIO may not support sub-day precision
        config = LifecycleConfig(
            [
                Rule(
                    status="Enabled",
                    rule_id="DeleteOldPhotos",
                    expiration=Expiration(days=1),  # TEMPORAL: 1 day for testing (change back to 3 after test)
                )
            ]
        )
        client.set_bucket_lifecycle(MINIO_BUCKET, config)
        print("Lifecycle policy (1 day TEMPORAL for testing) configured.")
        
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

def optimize_image(content: bytes, max_width: int = 1200, lossless: bool = True, quality: int = 80) -> bytes:
    """Resize and compress image to WebP."""
    try:
        img = Image.open(io.BytesIO(content))
        if img.mode in ("RGBA", "P"):
            img = img.convert("RGB")
        if img.width > max_width:
            ratio = max_width / float(img.width)
            new_height = int(float(img.height) * ratio)
            img = img.resize((max_width, new_height), Image.Resampling.LANCZOS)
            
        out_io = io.BytesIO()
        # Use lossless for photographers, lossy for speed in public gallery
        img.save(out_io, format="WEBP", lossless=lossless, quality=quality, method=6)
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
            # Default to high-fidelity (lossless) for direct uploads
            content = optimize_image(content, lossless=True)
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
                    # Public gallery: Speed is priority. Use lossy compression (quality 80)
                    opt_content = optimize_image(resp.content, max_width=1200, lossless=False, quality=80)
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
        
        # Keep only 15 items in public gallery
        if len(gallery) > 15:
            removed_item = gallery.pop()
            # Delete old webp file from MinIO to avoid orphan files
            if removed_item.get("edited_url"):
                old_filename = removed_item["edited_url"].split("/")[-1]
                if old_filename.startswith("gallery-"):
                    try:
                        client.remove_object(MINIO_BUCKET, old_filename)
                        print(f"[Gallery] Deleted old file: {old_filename}")
                    except Exception as del_err:
                        print(f"[Gallery] Warning: Could not delete old file: {del_err}")

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
    preset_name: str = "Solo Reescalado"
    preset_icon: str = "🔍"
    preset_description: str = ""
    original_download_url: str = ""
    delivery_id: str = ""

class UserErrorItem(BaseModel):
    user_id: str
    error_message: str
    execution_id: str = ""
    preset_name: str = "Unknown"
    refund_credits: bool = True
    credits_amount: int = 1

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

        # 1. Create Display Version (WebP Optimized)
        display_url = item.edited_url
        async with httpx.AsyncClient() as h_client:
            try:
                resp = await h_client.get(item.edited_url, timeout=30)
                if resp.status_code == 200:
                    # For Personal Gallery display: Use lossy quality 85 for balance
                    opt_content = optimize_image(resp.content, max_width=1200, lossless=False, quality=85)
                    filename = f"user_data/{item.user_id}/display-{uuid.uuid4()}.webp"
                    client.put_object(MINIO_BUCKET, filename, io.BytesIO(opt_content), len(opt_content), content_type="image/webp")
                    protocol = "https" if MINIO_SECURE else "http"
                    display_url = f"{protocol}://{MINIO_ENDPOINT}/{MINIO_BUCKET}/{filename}"
            except: pass

        new_entry = item.dict()
        new_entry["id"] = str(uuid.uuid4())
        new_entry["timestamp"] = time.time()
        new_entry["display_url"] = display_url
        # Use original_download_url if provided, otherwise use edited_url as fallback
        new_entry["original_download_url"] = item.original_download_url if item.original_download_url else item.edited_url
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
        gallery = json.loads(content)
        
        # Return gallery without automatic cleanup
        # Cleanup is handled by separate microservice
        return gallery
    except S3Error as e:
        if e.code == "NoSuchKey": return []
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/user-gallery/error")
async def log_user_error(item: UserErrorItem):
    try:
        # 1. Refund Credits if requested
        refund_status = "Not Requested"
        print(f"[Error] Processing refund request: user={item.user_id}, credits={item.credits_amount}, refund={item.refund_credits}")
        
        if item.refund_credits and SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY:
            try:
                async with httpx.AsyncClient() as h_client:
                    # We use RPC directly via Service Role to avoid RLS issues
                    rpc_url = f"{SUPABASE_URL}/rest/v1/rpc/refund_credits"
                    headers = {
                        "apikey": SUPABASE_SERVICE_ROLE_KEY,
                        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
                        "Content-Type": "application/json"
                    }
                    payload = {
                        "p_user_id": str(item.user_id),
                        "p_amount": int(item.credits_amount)
                    }
                    print(f"[Error] Calling refund_credits with payload: {payload}")
                    resp = await h_client.post(rpc_url, json=payload, timeout=10)
                    resp_text = resp.text
                    print(f"[Error] Refund response: {resp.status_code} - {resp_text}")
                    
                    if resp.status_code == 200:
                        refund_status = "Success"
                    else:
                        refund_status = f"Failed ({resp.status_code}): {resp_text}"
            except Exception as re:
                print(f"[Error] Refund exception: {str(re)}")
                refund_status = f"Error: {str(re)}"
        else:
            print(f"[Error] Refund skipped - refund_credits={item.refund_credits}, SUPABASE_URL={bool(SUPABASE_URL)}, SUPABASE_SERVICE_ROLE_KEY={bool(SUPABASE_SERVICE_ROLE_KEY)}")

        # 2. Log Error
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
        new_error["refund_status"] = refund_status
        errors.insert(0, new_error)
        
        if len(errors) > 20: errors.pop()

        data = json.dumps(errors).encode("utf-8")
        client.put_object(MINIO_BUCKET, path, io.BytesIO(data), len(data), content_type="application/json")
        return {"status": "success", "refund": refund_status}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class DeleteItemRequest(BaseModel):
    user_id: str
    item_id: str

@app.post("/user-gallery/delete-item")
async def delete_gallery_item(req: DeleteItemRequest):
    try:
        path = f"user_data/{req.user_id}/gallery.json"
        response = client.get_object(MINIO_BUCKET, path)
        gallery = json.loads(response.read())
        response.close()
        response.release_conn()

        # Find item
        item = next((x for x in gallery if x["id"] == req.item_id), None)
        if not item:
            raise HTTPException(status_code=404, detail="Item not found")

        # Delete objects from MinIO if they are local
        for key in ["display_url", "original_download_url"]:
            url = item.get(key, "")
            if f"{MINIO_ENDPOINT}/{MINIO_BUCKET}/" in url:
                obj_name = url.split(f"{MINIO_BUCKET}/")[-1]
                try: client.remove_object(MINIO_BUCKET, obj_name)
                except: pass

        # Remove from list
        new_gallery = [x for x in gallery if x["id"] != req.item_id]
        data = json.dumps(new_gallery).encode("utf-8")
        client.put_object(MINIO_BUCKET, path, io.BytesIO(data), len(data), content_type="application/json")
        
        return {"status": "success"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class DeleteErrorRequest(BaseModel):
    user_id: str
    error_index: int

@app.post("/user-gallery/delete-error")
async def delete_user_error(req: DeleteErrorRequest):
    try:
        path = f"user_data/{req.user_id}/errors.json"
        response = client.get_object(MINIO_BUCKET, path)
        errors = json.loads(response.read())
        response.close()
        response.release_conn()

        if 0 <= req.error_index < len(errors):
            errors.pop(req.error_index)
        else:
            raise HTTPException(status_code=400, detail="Invalid error index")

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
            "expiry_hours": IMAGE_EXPIRY_HOURS
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/download/{filename}")
async def download_file(filename: str):
    """Force download of a file from MinIO."""
    try:
        # Get object from MinIO
        response = client.get_object(MINIO_BUCKET, filename)
        
        # Determine clean filename for the user
        # Remove UUID prefix (36 chars + dash) if present
        clean_name = filename
        if len(filename) > 37 and filename[36] == '-':
            clean_name = filename[37:]

        return StreamingResponse(
            response,
            media_type="application/octet-stream",
            headers={
                "Content-Disposition": f'attachment; filename="{clean_name}"'
            }
        )
    except S3Error as e:
        if e.code == "NoSuchKey":
            raise HTTPException(status_code=404, detail="File not found")
        raise HTTPException(status_code=500, detail=str(e))
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
