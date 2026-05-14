#!/usr/bin/env python3

"""
Ingest county finance documents from the normalized S3 raw/ structure.

Supported extraction modes:
  auto      - direct text for text files, pypdf for PDFs, Textract fallback
  pypdf     - pypdf/direct text only
  textract  - AWS Textract only

Expected S3 raw key pattern:
  raw/state=GA/county_fips=13191/county=mcintosh/fiscal_year=2026/document_type=budget/source=county_website/FY26-Operating-Budget.pdf
"""

import argparse
import hashlib
import json
import os
import re
import sys
import tempfile
import time
from pathlib import PurePosixPath
from typing import Dict, Optional, Tuple

import boto3
import psycopg2
from psycopg2.extras import RealDictCursor

try:
    from pypdf import PdfReader
except Exception:
    PdfReader = None


# ---------------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------------

try:
    sys.path.append("/home/ec2-user")
    from app import DB_CONFIG
except Exception:
    DB_CONFIG = {
        "host": os.environ.get("DB_HOST"),
        "port": int(os.environ.get("DB_PORT", "5432")),
        "dbname": os.environ.get("DB_NAME", "postgres"),
        "user": os.environ.get("DB_USER", "postgres"),
        "password": os.environ.get("DB_PASSWORD"),
        "sslmode": os.environ.get("DB_SSLMODE", "require"),
        "connect_timeout": int(os.environ.get("DB_CONNECT_TIMEOUT", "5")),
    }


DEFAULT_BUCKET = "county-finance-docs"
DEFAULT_PREFIX = "raw/"
PROCESSED_TEXT_PREFIX = "processed/text"

SUPPORTED_EXTENSIONS = {
    ".pdf",
    ".txt",
    ".csv",
    ".json",
    ".md",
    ".jpg",
    ".jpeg",
    ".png",
    ".tif",
    ".tiff",
}

TEXT_EXTENSIONS = {
    ".txt",
    ".csv",
    ".json",
    ".md",
}


# ---------------------------------------------------------------------
# S3 KEY PARSING
# ---------------------------------------------------------------------

def slug_to_title(filename: str) -> str:
    name = PurePosixPath(filename).name
    name = re.sub(r"[-_]+", " ", name)
    return name.strip()


def parse_partitioned_raw_key(s3_key: str) -> Dict:
    parts = s3_key.split("/")

    if not parts or parts[0] != "raw":
        raise ValueError(f"Not a normalized raw key: {s3_key}")

    metadata = {
        "lifecycle_stage": "raw",
        "s3_key": s3_key,
        "filename": PurePosixPath(s3_key).name,
    }

    for part in parts[1:-1]:
        if "=" in part:
            key, value = part.split("=", 1)
            metadata[key] = value

    required = [
        "state",
        "county_fips",
        "county",
        "fiscal_year",
        "document_type",
    ]

    missing = [field for field in required if not metadata.get(field)]
    if missing:
        raise ValueError(f"Missing required fields {missing} in S3 key: {s3_key}")

    metadata["fiscal_year"] = int(metadata["fiscal_year"])
    metadata["source"] = metadata.get("source", "unknown")
    metadata["title"] = slug_to_title(metadata["filename"])

    return metadata


# ---------------------------------------------------------------------
# DATABASE SETUP / COMPATIBILITY
# ---------------------------------------------------------------------

def ensure_schema(cur):
    """
    Safe to rerun.
    Adds comparison-aware columns needed by the normalized raw/ ingestion pipeline.
    """

    cur.execute("CREATE EXTENSION IF NOT EXISTS pgcrypto;")
    cur.execute("CREATE SCHEMA IF NOT EXISTS reference;")

    cur.execute("""
        CREATE TABLE IF NOT EXISTS reference.jurisdictions (
            id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            state text NOT NULL,
            county_name text NOT NULL,
            display_name text NOT NULL,
            fips_code text,
            population integer,
            is_active boolean NOT NULL DEFAULT true,
            created_at timestamptz NOT NULL DEFAULT now(),
            UNIQUE (state, fips_code),
            UNIQUE (state, county_name)
        );
    """)

    cur.execute("""
        ALTER TABLE registry.documents
        ADD COLUMN IF NOT EXISTS jurisdiction_id uuid REFERENCES reference.jurisdictions(id),
        ADD COLUMN IF NOT EXISTS fiscal_year integer,
        ADD COLUMN IF NOT EXISTS canonical_s3_bucket text,
        ADD COLUMN IF NOT EXISTS canonical_s3_key text,
        ADD COLUMN IF NOT EXISTS source_system text,
        ADD COLUMN IF NOT EXISTS source_note text;
    """)

    cur.execute("""
        ALTER TABLE registry.document_versions
        ADD COLUMN IF NOT EXISTS s3_bucket text,
        ADD COLUMN IF NOT EXISTS sha256_hash text,
        ADD COLUMN IF NOT EXISTS content_type text,
        ADD COLUMN IF NOT EXISTS etag text,
        ADD COLUMN IF NOT EXISTS last_modified timestamptz;
    """)

    cur.execute("""
        ALTER TABLE registry.document_text
        ADD COLUMN IF NOT EXISTS processed_s3_bucket text,
        ADD COLUMN IF NOT EXISTS processed_s3_key text,
        ADD COLUMN IF NOT EXISTS page_count integer,
        ADD COLUMN IF NOT EXISTS char_count integer,
        ADD COLUMN IF NOT EXISTS extraction_status text,
        ADD COLUMN IF NOT EXISTS extraction_metadata jsonb NOT NULL DEFAULT '{}'::jsonb;
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_document_versions_bucket_key_hash
        ON registry.document_versions (s3_bucket, s3_key, sha256_hash);
    """)

    cur.execute("""
        CREATE UNIQUE INDEX IF NOT EXISTS ux_document_text_document_version
        ON registry.document_text (document_version_id);
    """)


def get_or_create_jurisdiction(cur, meta: Dict) -> str:
    county_name = meta["county"].replace("-", " ").title()
    display_name = f"{county_name} County, {meta['state']}"

    cur.execute(
        """
        SELECT id
        FROM reference.jurisdictions
        WHERE state = %s
          AND fips_code = %s;
        """,
        (meta["state"], meta["county_fips"]),
    )

    row = cur.fetchone()

    if row:
        return row["id"]

    cur.execute(
        """
        INSERT INTO reference.jurisdictions (
            state,
            county_name,
            display_name,
            fips_code
        )
        VALUES (%s, %s, %s, %s)
        RETURNING id;
        """,
        (
            meta["state"],
            county_name,
            display_name,
            meta["county_fips"],
        ),
    )

    return cur.fetchone()["id"]


def upsert_document(cur, *, bucket: str, key: str, meta: Dict, jurisdiction_id: str) -> str:
    cur.execute(
        """
        SELECT id
        FROM registry.documents
        WHERE canonical_s3_bucket = %s
          AND canonical_s3_key = %s;
        """,
        (bucket, key),
    )

    row = cur.fetchone()

    if row:
        document_id = row["id"]

        cur.execute(
            """
            UPDATE registry.documents
            SET
                title = %s,
                document_type = %s,
                jurisdiction_id = %s,
                fiscal_year = %s,
                source_system = %s,
                source_note = %s
            WHERE id = %s;
            """,
            (
                meta["title"],
                meta["document_type"],
                jurisdiction_id,
                meta["fiscal_year"],
                meta.get("source"),
                "normalized raw S3 ingestion",
                document_id,
            ),
        )

        return document_id

    cur.execute(
        """
        INSERT INTO registry.documents (
            title,
            document_type,
            jurisdiction_id,
            fiscal_year,
            canonical_s3_bucket,
            canonical_s3_key,
            source_system,
            source_note
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
        """,
        (
            meta["title"],
            meta["document_type"],
            jurisdiction_id,
            meta["fiscal_year"],
            bucket,
            key,
            meta.get("source"),
            "normalized raw S3 ingestion",
        ),
    )

    return cur.fetchone()["id"]


def upsert_document_version(
    cur,
    *,
    document_id: str,
    bucket: str,
    key: str,
    obj: Dict,
    sha256_hash: str,
    file_size_bytes: int,
    content_type: Optional[str],
) -> str:
    """
    Upserts a document version.

    Compatibility note:
    Your existing registry.document_versions table has version_number NOT NULL.
    This function always supplies version_number and maintains is_current/current_version_id.
    """

    cur.execute(
        """
        SELECT id
        FROM registry.document_versions
        WHERE s3_bucket = %s
          AND s3_key = %s
          AND sha256_hash = %s;
        """,
        (bucket, key, sha256_hash),
    )

    row = cur.fetchone()

    if row:
        version_id = row["id"]

        cur.execute(
            """
            UPDATE registry.document_versions
            SET is_current = false
            WHERE document_id = %s
              AND id <> %s;
            """,
            (document_id, version_id),
        )

        cur.execute(
            """
            UPDATE registry.document_versions
            SET is_current = true
            WHERE id = %s;
            """,
            (version_id,),
        )

    else:
        cur.execute(
            """
            SELECT COALESCE(MAX(version_number), 0) + 1 AS next_version
            FROM registry.document_versions
            WHERE document_id = %s;
            """,
            (document_id,),
        )

        next_version = cur.fetchone()["next_version"]

        cur.execute(
            """
            UPDATE registry.document_versions
            SET is_current = false
            WHERE document_id = %s;
            """,
            (document_id,),
        )

        cur.execute(
            """
            INSERT INTO registry.document_versions (
                document_id,
                version_number,
                is_current,
                s3_bucket,
                s3_key,
                sha256_hash,
                file_size_bytes,
                content_type,
                etag,
                last_modified
            )
            VALUES (%s, %s, true, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id;
            """,
            (
                document_id,
                next_version,
                bucket,
                key,
                sha256_hash,
                file_size_bytes,
                content_type,
                obj.get("ETag", "").replace('"', ""),
                obj.get("LastModified"),
            ),
        )

        version_id = cur.fetchone()["id"]

    cur.execute(
        """
        UPDATE registry.documents
        SET current_version_id = %s
        WHERE id = %s;
        """,
        (version_id, document_id),
    )

    return version_id


def existing_text_status(cur, document_version_id: str):
    cur.execute(
        """
        SELECT
            extraction_method,
            char_count,
            extraction_status
        FROM registry.document_text
        WHERE document_version_id = %s;
        """,
        (document_version_id,),
    )

    return cur.fetchone()


def upsert_document_text(
    cur,
    *,
    document_version_id: str,
    extraction_method: str,
    full_text: str,
    processed_bucket: Optional[str],
    processed_key: Optional[str],
    page_count: Optional[int],
    status: str,
    metadata: Dict,
):
    cur.execute(
        """
        INSERT INTO registry.document_text (
            document_version_id,
            extraction_method,
            full_text,
            processed_s3_bucket,
            processed_s3_key,
            page_count,
            char_count,
            extraction_status,
            extraction_metadata
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
        ON CONFLICT (document_version_id)
        DO UPDATE SET
            extraction_method = EXCLUDED.extraction_method,
            full_text = EXCLUDED.full_text,
            processed_s3_bucket = EXCLUDED.processed_s3_bucket,
            processed_s3_key = EXCLUDED.processed_s3_key,
            page_count = EXCLUDED.page_count,
            char_count = EXCLUDED.char_count,
            extraction_status = EXCLUDED.extraction_status,
            extraction_metadata = EXCLUDED.extraction_metadata;
        """,
        (
            document_version_id,
            extraction_method,
            full_text,
            processed_bucket,
            processed_key,
            page_count,
            len(full_text or ""),
            status,
            json.dumps(metadata),
        ),
    )


# ---------------------------------------------------------------------
# S3 HELPERS
# ---------------------------------------------------------------------

def iter_s3_objects(s3, bucket: str, prefix: str, limit: Optional[int] = None):
    paginator = s3.get_paginator("list_objects_v2")
    count = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]

            if key.endswith("/"):
                continue

            ext = PurePosixPath(key).suffix.lower()

            if ext and ext not in SUPPORTED_EXTENSIONS:
                print(f"SKIP unsupported extension: {key}")
                continue

            yield obj

            count += 1

            if limit and count >= limit:
                return


def download_s3_object(s3, bucket: str, key: str) -> Tuple[str, str, int, Optional[str], Dict]:
    response = s3.get_object(Bucket=bucket, Key=key)
    content_type = response.get("ContentType")
    body = response["Body"]

    suffix = PurePosixPath(key).suffix.lower()
    fd, path = tempfile.mkstemp(suffix=suffix)
    os.close(fd)

    sha = hashlib.sha256()
    size = 0

    with open(path, "wb") as f:
        while True:
            chunk = body.read(1024 * 1024)
            if not chunk:
                break

            sha.update(chunk)
            size += len(chunk)
            f.write(chunk)

    return path, sha.hexdigest(), size, content_type, response


def write_processed_text_to_s3(
    s3,
    *,
    bucket: str,
    raw_key: str,
    document_version_id: str,
    text: str,
) -> str:
    processed_key = f"{PROCESSED_TEXT_PREFIX}/document_version_id={document_version_id}/full_text.txt"

    s3.put_object(
        Bucket=bucket,
        Key=processed_key,
        Body=(text or "").encode("utf-8"),
        ContentType="text/plain; charset=utf-8",
        Metadata={
            "source_raw_key": raw_key[:1024],
            "document_version_id": str(document_version_id),
        },
    )

    return processed_key


# ---------------------------------------------------------------------
# EXTRACTION BLOCK: TEXT FILES
# ---------------------------------------------------------------------

def extract_text_direct(local_path: str) -> Tuple[str, Dict]:
    try:
        with open(local_path, "r", encoding="utf-8", errors="replace") as f:
            text = f.read()

        return text, {
            "ok": True,
            "method": "direct_text",
            "page_count": None,
            "char_count": len(text),
        }

    except Exception as e:
        return "", {
            "ok": False,
            "method": "direct_text",
            "reason": str(e),
            "page_count": None,
        }


# ---------------------------------------------------------------------
# EXTRACTION BLOCK: PYPDF
# ---------------------------------------------------------------------

def extract_text_pypdf(local_path: str) -> Tuple[str, Dict]:
    if PdfReader is None:
        return "", {
            "ok": False,
            "method": "pypdf",
            "reason": "pypdf not installed",
            "page_count": None,
        }

    try:
        reader = PdfReader(local_path)
        texts = []

        for idx, page in enumerate(reader.pages):
            try:
                page_text = page.extract_text() or ""
            except Exception as e:
                page_text = f"\n[PAGE {idx + 1} EXTRACTION ERROR: {e}]\n"

            texts.append(f"\n--- PAGE {idx + 1} ---\n{page_text}")

        full_text = "\n".join(texts).strip()

        return full_text, {
            "ok": True,
            "method": "pypdf",
            "page_count": len(reader.pages),
            "char_count": len(full_text),
        }

    except Exception as e:
        return "", {
            "ok": False,
            "method": "pypdf",
            "reason": str(e),
            "page_count": None,
        }


# ---------------------------------------------------------------------
# EXTRACTION BLOCK: AWS TEXTRACT
# ---------------------------------------------------------------------

def extract_text_textract(
    textract,
    *,
    bucket: str,
    key: str,
    poll_seconds: int = 5,
    timeout_seconds: int = 900,
) -> Tuple[str, Dict]:
    """
    AWS Textract async document text detection.

    Requires IAM:
      textract:StartDocumentTextDetection
      textract:GetDocumentTextDetection
      s3:GetObject on the source object
    """

    try:
        start = textract.start_document_text_detection(
            DocumentLocation={
                "S3Object": {
                    "Bucket": bucket,
                    "Name": key,
                }
            }
        )

        job_id = start["JobId"]
        deadline = time.time() + timeout_seconds

        blocks = []
        next_token = None
        status = None

        while True:
            if time.time() > deadline:
                return "", {
                    "ok": False,
                    "method": "textract",
                    "job_id": job_id,
                    "reason": "Textract timeout",
                }

            kwargs = {"JobId": job_id}

            if next_token:
                kwargs["NextToken"] = next_token

            response = textract.get_document_text_detection(**kwargs)
            status = response["JobStatus"]

            if status == "IN_PROGRESS":
                time.sleep(poll_seconds)
                continue

            if status != "SUCCEEDED":
                return "", {
                    "ok": False,
                    "method": "textract",
                    "job_id": job_id,
                    "status": status,
                    "reason": response.get("StatusMessage"),
                }

            blocks.extend(response.get("Blocks", []))
            next_token = response.get("NextToken")

            if not next_token:
                break

        lines = []

        for block in blocks:
            if block.get("BlockType") == "LINE":
                page = block.get("Page")
                text = block.get("Text", "")
                lines.append((page, text))

        output = []
        current_page = None

        for page, text in lines:
            if page != current_page:
                current_page = page
                output.append(f"\n--- PAGE {page} TEXTRACT ---")

            output.append(text)

        full_text = "\n".join(output).strip()

        return full_text, {
            "ok": True,
            "method": "textract",
            "job_id": job_id,
            "status": status,
            "page_count": max([p for p, _ in lines], default=None),
            "line_count": len(lines),
            "char_count": len(full_text),
        }

    except Exception as e:
        return "", {
            "ok": False,
            "method": "textract",
            "reason": str(e),
        }


# ---------------------------------------------------------------------
# EXTRACTION ROUTER
# ---------------------------------------------------------------------

def extract_text(
    *,
    strategy: str,
    local_path: str,
    bucket: str,
    key: str,
    textract_client,
    min_chars: int = 500,
) -> Tuple[str, str, Dict]:
    """
    strategy:
      auto
      pypdf
      textract

    auto behavior:
      text/csv/json/md -> direct read
      pdf              -> pypdf first, Textract fallback if weak/no text
      image/tiff       -> Textract

    Important:
      Do not attach an attempts list that contains the same dict being returned.
      That creates a circular reference and breaks json.dumps().
    """

    ext = PurePosixPath(local_path).suffix.lower()

    if strategy == "pypdf":
        if ext == ".pdf":
            text, meta = extract_text_pypdf(local_path)
            return text, "pypdf", meta

        if ext in TEXT_EXTENSIONS:
            text, meta = extract_text_direct(local_path)
            return text, "direct_text", meta

        return "", "none", {
            "ok": False,
            "method": "pypdf",
            "reason": f"pypdf does not support extension: {ext}",
        }

    if strategy == "textract":
        text, meta = extract_text_textract(
            textract_client,
            bucket=bucket,
            key=key,
        )
        return text, "textract", meta

    if strategy != "auto":
        raise ValueError(f"Unknown extraction strategy: {strategy}")

    attempts = []

    if ext in TEXT_EXTENSIONS:
        text, meta = extract_text_direct(local_path)
        return text, "direct_text", {
            **meta,
            "attempts": [dict(meta)],
        }

    if ext == ".pdf":
        pypdf_text, pypdf_meta = extract_text_pypdf(local_path)
        attempts.append(dict(pypdf_meta))

        if pypdf_meta.get("ok") and len(pypdf_text or "") >= min_chars:
            return pypdf_text, "pypdf", {
                **pypdf_meta,
                "attempts": attempts,
            }

        textract_text, textract_meta = extract_text_textract(
            textract_client,
            bucket=bucket,
            key=key,
        )
        attempts.append(dict(textract_meta))

        if textract_meta.get("ok"):
            return textract_text, "textract", {
                **textract_meta,
                "attempts": attempts,
            }

        return "", "none", {
            "ok": False,
            "method": "auto",
            "attempts": attempts,
        }

    if ext in {".jpg", ".jpeg", ".png", ".tif", ".tiff"}:
        textract_text, textract_meta = extract_text_textract(
            textract_client,
            bucket=bucket,
            key=key,
        )
        attempts.append(dict(textract_meta))

        if textract_meta.get("ok"):
            return textract_text, "textract", {
                **textract_meta,
                "attempts": attempts,
            }

        return "", "none", {
            "ok": False,
            "method": "auto",
            "attempts": attempts,
        }

    return "", "none", {
        "ok": False,
        "method": "auto",
        "reason": f"unsupported extension: {ext}",
    }


# ---------------------------------------------------------------------
# INGEST ONE OBJECT
# ---------------------------------------------------------------------

def ingest_object(
    *,
    s3,
    textract,
    conn,
    bucket: str,
    obj: Dict,
    extractor: str,
    write_processed_text: bool,
    skip_existing_text: bool,
    dry_run: bool,
):
    key = obj["Key"]

    print(f"\nProcessing: s3://{bucket}/{key}")

    try:
        meta = parse_partitioned_raw_key(key)
    except Exception as e:
        print(f"SKIP bad key: {e}")
        return {
            "key": key,
            "status": "skipped_bad_key",
            "error": str(e),
        }

    local_path = None

    try:
        local_path, sha256_hash, file_size_bytes, content_type, response = download_s3_object(
            s3,
            bucket,
            key,
        )

        if dry_run:
            print(f"DRY RUN metadata: {meta}")
            print(f"DRY RUN sha256: {sha256_hash}")
            print(f"DRY RUN size: {file_size_bytes:,} bytes")
            return {
                "key": key,
                "status": "dry_run",
            }

        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            jurisdiction_id = get_or_create_jurisdiction(cur, meta)

            document_id = upsert_document(
                cur,
                bucket=bucket,
                key=key,
                meta=meta,
                jurisdiction_id=jurisdiction_id,
            )

            version_id = upsert_document_version(
                cur,
                document_id=document_id,
                bucket=bucket,
                key=key,
                obj=response,
                sha256_hash=sha256_hash,
                file_size_bytes=file_size_bytes,
                content_type=content_type,
            )

            if skip_existing_text:
                existing = existing_text_status(cur, version_id)

                if existing and existing.get("char_count") and existing["char_count"] > 0:
                    conn.commit()

                    print(
                        f"SKIP existing text: "
                        f"method={existing['extraction_method']}, "
                        f"chars={existing['char_count']:,}"
                    )

                    return {
                        "key": key,
                        "status": "skipped_existing_text",
                        "method": existing["extraction_method"],
                        "chars": existing["char_count"],
                    }

            text, method, extraction_meta = extract_text(
                strategy=extractor,
                local_path=local_path,
                bucket=bucket,
                key=key,
                textract_client=textract,
            )

            processed_key = None

            if write_processed_text and text:
                processed_key = write_processed_text_to_s3(
                    s3,
                    bucket=bucket,
                    raw_key=key,
                    document_version_id=version_id,
                    text=text,
                )

            status = "extracted" if text else "no_text"

            upsert_document_text(
                cur,
                document_version_id=version_id,
                extraction_method=method,
                full_text=text or "",
                processed_bucket=bucket if processed_key else None,
                processed_key=processed_key,
                page_count=extraction_meta.get("page_count"),
                status=status,
                metadata={
                    "raw_s3_bucket": bucket,
                    "raw_s3_key": key,
                    "parsed_metadata": meta,
                    "extractor_requested": extractor,
                    "extraction": extraction_meta,
                    "sha256_hash": sha256_hash,
                    "file_size_bytes": file_size_bytes,
                    "content_type": content_type,
                },
            )

            conn.commit()

        print(f"OK: method={method}, status={status}, chars={len(text or ''):,}")

        return {
            "key": key,
            "status": "ok",
            "method": method,
            "chars": len(text or ""),
        }

    except Exception as e:
        conn.rollback()
        print(f"ERROR: {key}: {e}")

        return {
            "key": key,
            "status": "error",
            "error": str(e),
        }

    finally:
        if local_path and os.path.exists(local_path):
            try:
                os.remove(local_path)
            except Exception:
                pass


# ---------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Ingest county finance documents from normalized S3 raw/ structure."
    )

    parser.add_argument("--bucket", default=DEFAULT_BUCKET)
    parser.add_argument("--prefix", default=DEFAULT_PREFIX)
    parser.add_argument("--extractor", choices=["auto", "pypdf", "textract"], default="auto")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--write-processed-text", action="store_true")
    parser.add_argument("--skip-existing-text", action="store_true")
    parser.add_argument("--region", default=os.environ.get("AWS_REGION", "us-east-1"))

    args = parser.parse_args()

    session = boto3.Session(region_name=args.region)
    s3 = session.client("s3")
    textract = session.client("textract")

    results = []

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            ensure_schema(cur)
            conn.commit()

        for obj in iter_s3_objects(s3, args.bucket, args.prefix, args.limit):
            result = ingest_object(
                s3=s3,
                textract=textract,
                conn=conn,
                bucket=args.bucket,
                obj=obj,
                extractor=args.extractor,
                write_processed_text=args.write_processed_text,
                skip_existing_text=args.skip_existing_text,
                dry_run=args.dry_run,
            )

            results.append(result)

    print("\n--- SUMMARY ---")

    summary = {}

    for result in results:
        summary[result["status"]] = summary.get(result["status"], 0) + 1

    print(json.dumps(summary, indent=2))

    errors = [result for result in results if result["status"] == "error"]

    if errors:
        print("\nErrors:")

        for error in errors:
            print(f"- {error['key']}: {error.get('error')}")

    print("\nDone.")


if __name__ == "__main__":
    main()
