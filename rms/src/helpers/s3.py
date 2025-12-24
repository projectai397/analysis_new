from datetime import datetime
import os
import requests
from pathlib import Path
import shutil
import subprocess
from venv import logger
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from src import config
from dotenv import load_dotenv

load_dotenv()

def _which(cmds: list[str]) -> str | None:
    """Return the first executable found in PATH from a list of candidate names."""
    for c in cmds:
        p = shutil.which(c)
        if p:
            return p
    return None


def backup_mongo_to_archive(
    db_names: list[str],
    mongo_uri: str | None = None,
    out_root: str = "backups",
) -> dict:
    """
    Dumps each collection for the given databases into BSON files
    and archives the dump folder into YYYY-MM-DD.zip.

    Returns:
        {
          "ok": bool,
          "date": "YYYY-MM-DD",
          "dump_dir": "<absolute path>",
          "archive_path": "<absolute path to .zip>",
          "used_format": "zip",
          "error": "<message-if-any>"
        }
    """
    date_str = datetime.now().strftime("%Y-%m-%d")
    root = Path(out_root).resolve()
    dump_dir = root / date_str  # e.g., backups/2025-10-28
    dump_dir.mkdir(parents=True, exist_ok=True)

    uri = os.environ.get("SOURCE_MONGO_URI")
    if not uri:
        return {"ok": False, "date": date_str, "dump_dir": str(dump_dir),
                "archive_path": None, "used_format": None,
                "error": "Mongo URI not set (config.SRC_MONGO_URI or MONGO_URI)"}

    mongodump = shutil.which("mongodump")
    if not mongodump:
        return {"ok": False, "date": date_str, "dump_dir": str(dump_dir),
                "archive_path": None, "used_format": None,
                "error": "mongodump not found in PATH. Install MongoDB Database Tools."}

    
    try:
        for db in db_names:
            cmd = [mongodump, f"--uri={uri}", f"--db={db}", f"--out={str(dump_dir)}"]
            logger.info(f"[backup] Running: {' '.join(cmd)}")
            subprocess.run(cmd, check=True)
    except subprocess.CalledProcessError as e:
        return {"ok": False, "date": date_str, "dump_dir": str(dump_dir),
                "archive_path": None, "used_format": None,
                "error": f"mongodump failed for db '{db}': {e}"}

    # 2️⃣ Always create ZIP archive
    zip_base = root / date_str
    archive_path = zip_base.with_suffix(".zip")
    logger.info(f"[backup] Creating ZIP archive → {archive_path}")
    shutil.make_archive(str(zip_base), "zip", str(dump_dir))

    return {
        "ok": True,
        "date": date_str,
        "dump_dir": str(dump_dir),
        "archive_path": str(archive_path),
        "used_format": "zip",
        "error": None,
    }
def _s3_client():
    """Build a boto3 S3 client from env or config."""
    access_key = os.environ.get("AWS_ACCESS_KEY_ID") or getattr(config, "AWS_ACCESS_KEY_ID", None)
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY") or getattr(config, "AWS_SECRET_ACCESS_KEY", None)
    region     = os.environ.get("AWS_REGION") or getattr(config, "AWS_REGION", "ap-south-1")
    if not (access_key and secret_key):
        raise RuntimeError("AWS creds not set (AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY).")
    return boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

def upload_backup_to_s3(
    date_str: str | None = None,
    out_root: str = "backups",
    bucket: str | None = None,
    s3_prefix: str = "mongo_backup"
) -> dict:
    """
    Uploads the backup archive for date_str to S3.
    Hits notification API on success using credentials from .env.
    """
    date_str = date_str or datetime.now().strftime("%Y-%m-%d")
    root = Path(out_root).resolve()

    archive_path = root / f"{date_str}.zip"
    if not archive_path.exists():
        return {"ok": False, "date": date_str, "archive_path": None,
                "bucket": bucket, "key": None,
                "error": f"No ZIP archive found for {date_str} at {archive_path}"}

    bucket = bucket or os.environ.get("S3_BUCKET") or getattr(config, "S3_BUCKET", None)
    if not bucket:
        return {"ok": False, "date": date_str, "archive_path": str(archive_path),
                "bucket": None, "key": None, "error": "S3 bucket not set (S3_BUCKET)"}

    key = f"{s3_prefix}/{archive_path.name}"

    try:
        s3 = _s3_client()
        logger.info(f"[backup] Uploading {archive_path} → s3://{bucket}/{key}")
        s3.upload_file(str(archive_path), bucket, key)

        # ─── NOTIFICATION LOGIC ───
        try:
            # Fetching from .env as requested
            notif_url = os.environ.get("NOTIFICATION_URL")
            auth_token = os.environ.get("STATIC_TOKEN") 
            
            if notif_url and auth_token:
                headers = {
                    "X-Auth-Token": auth_token,
                    "Content-Type": "application/json"
                }
                payload = {
                    "message": "database upload complete"
                }

                notif_res = requests.post(notif_url, json=payload, headers=headers, timeout=10)
                if notif_res.status_code == 200:
                    logger.info("✔ Notification sent: database upload complete")
                else:
                    logger.warning(f"✖ Notification failed (Status {notif_res.status_code}): {notif_res.text}")
            else:
                logger.error("✖ Missing NOTIFICATION_URL or STATIC_TOKEN in .env")
                
        except Exception as e:
            logger.error(f"✖ Notification trigger crashed: {e}")
        # ──────────────────────────

        return {
            "ok": True,
            "date": date_str,
            "archive_path": str(archive_path),
            "bucket": bucket,
            "key": key,
            "error": None
        }

    except FileNotFoundError:
        return {"ok": False, "date": date_str, "archive_path": str(archive_path),
                "bucket": bucket, "key": key, "error": "Archive file not found"}
    except NoCredentialsError:
        return {"ok": False, "date": date_str, "archive_path": str(archive_path),
                "bucket": bucket, "key": key, "error": "AWS credentials not found/invalid"}
    except ClientError as e:
        return {"ok": False, "date": date_str, "archive_path": str(archive_path),
                "bucket": bucket, "key": key, "error": f"AWS error: {e}"}
    except Exception as e:
        return {"ok": False, "date": date_str, "archive_path": str(archive_path),
                "bucket": bucket, "key": key, "error": str(e)}

def download_backup_from_s3(
    date_str: str | None = None,
    out_root: str = "backups",
    bucket: str | None = None,
    s3_prefix: str = "mongo_backup"
) -> dict:
    """
    Downloads the backup archive from S3 to the local machine.
    
    Returns:
      { ok, date, archive_path, bucket, key, downloaded_to, error }
    """
    # Get the local download directory from the environment variable
    local_download_dir = os.getenv("LOCAL_DOWNLOAD_DIR", "downloads")  # Default to "downloads" if not set

    # Default date_str to today if None
    date_str = date_str or datetime.now().strftime("%Y-%m-%d")
    root = Path(out_root).resolve()

    archive_path = root / f"{date_str}.zip"
    if not archive_path.exists():
        return {"ok": False, "date": date_str, "archive_path": None,
                "bucket": bucket, "key": None,
                "error": f"No ZIP archive found for {date_str} at {archive_path}"}

    bucket = bucket or os.environ.get("S3_BUCKET") or getattr(config, "S3_BUCKET", None)
    if not bucket:
        return {"ok": False, "date": date_str, "archive_path": str(archive_path),
                "bucket": None, "key": None, "error": "S3 bucket not set (S3_BUCKET)"}

    key = f"{s3_prefix}/{archive_path.name}"  # e.g. mongo_update/2025-10-28.zip

    try:
        s3 = boto3.client('s3')  # Assuming your AWS credentials are set
        logger.info(f"[backup] Downloading {key} from S3 to {local_download_dir}/{archive_path.name}")

        # Ensure the local download directory exists
        download_path = Path(local_download_dir) / archive_path.name
        download_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Download from S3 to local path
        s3.download_file(bucket, key, str(download_path))

        return {
            "ok": True,
            "date": date_str,
            "archive_path": str(archive_path),
            "bucket": bucket,
            "key": key,
            "downloaded_to": str(download_path),
            "error": None
        }

    except FileNotFoundError:
        return {"ok": False, "date": date_str, "archive_path": str(archive_path),
                "bucket": bucket, "key": key, "error": "Archive file not found"}
    except NoCredentialsError:
        return {"ok": False, "date": date_str, "archive_path": str(archive_path),
                "bucket": bucket, "key": key, "error": "AWS credentials not found/invalid"}
    except ClientError as e:
        return {"ok": False, "date": date_str, "archive_path": str(archive_path),
                "bucket": bucket, "key": key, "error": f"AWS error: {e}"}
    except Exception as e:
        return {"ok": False, "date": date_str, "archive_path": str(archive_path),
                "bucket": bucket, "key": key, "error": str(e)}