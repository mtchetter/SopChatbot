# word_upsert.py
# DOCX -> chunks -> OpenAI embeddings -> Pinecone upsert -> archive
# Fixes:
# - Forces OpenAI client to use certifi CA bundle (avoids truststore/SSL hang)
# - Adds explicit timeouts and light retries
# - Uses new Pinecone SDK correctly (list_indexes -> list[str])

import os
import time
import uuid
import glob
import shutil
from typing import List, Dict, Any

from docx import Document
from pinecone import Pinecone
from openai import OpenAI

import httpx, certifi   # <- important: certifi CA + httpx client for OpenAI


# ---------- Configuration ----------
SOP_FOLDER = r"C:\Users\mtschetter\Desktop\all_sop"
ARCHIVE_SUBFOLDER = "Archive"
PINECONE_INDEX_NAME = "hr-data"

# API keys (hardcoded per your request)
PINECONE_API_KEY = "pcsk_6baxNS_TqwDk9caEhdTQDWXz4XB3Dxbuxr6N8m1P7KRRMvfmPxq1BA2DjfnAjpbQqRZWah"
OPENAI_API_KEY   = "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"

EMBED_MODEL = "text-embedding-3-large"

# Chunking params
CHUNK_SIZE = 2000
PARA_SEGMENT_SIZE = 500
PARA_SEGMENT_STEP = 400
MIN_CHUNK_TO_KEEP = 100
CHUNK_OVERLAP = 75

# Batching / pacing
BATCH_SIZE = 20
EMBED_DELAY_SEC = 0.5
BATCH_DELAY_SEC = 3

# Networking timeouts
HTTP_TIMEOUT_SECS = 60.0


# ---------- Helpers ----------
def log(msg: str) -> None:
    print(msg, flush=True)


def format_paragraph_references(paragraph_numbers: List[int]) -> str:
    if not paragraph_numbers:
        return "Unknown paragraph"
    paragraph_numbers = sorted(paragraph_numbers)
    groups = []
    current = [paragraph_numbers[0]]
    for i in range(1, len(paragraph_numbers)):
        if paragraph_numbers[i] == paragraph_numbers[i - 1] + 1:
            current.append(paragraph_numbers[i])
        else:
            groups.append(current)
            current = [paragraph_numbers[i]]
    groups.append(current)
    formatted = []
    for g in groups:
        if len(g) == 1:
            formatted.append(f"para.{g[0]}")
        else:
            formatted.append(f"paras.{g[0]}-{g[-1]}")
    return ", ".join(formatted)


def split_paragraph(text: str, size: int, step: int) -> List[str]:
    if not text:
        return []
    segments, i, n = [], 0, len(text)
    while i < n:
        segments.append(text[i:i + size])
        if i + size >= n:
            break
        i += step
    return segments


def chunk_paragraphs(paragraphs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    chunks = []
    current_text = ""
    current_paras = set()

    for p in paragraphs:
        pnum = p["paragraph_number"]
        ptext = p["text"]

        segments = split_paragraph(ptext, PARA_SEGMENT_SIZE, PARA_SEGMENT_STEP)
        if not segments:
            continue

        for seg in segments:
            if len(current_text) + len(seg) > CHUNK_SIZE and len(current_text) >= MIN_CHUNK_TO_KEEP:
                chunks.append({"text": current_text, "paragraphs": sorted(list(current_paras))})
                tail = current_text[-CHUNK_OVERLAP:] if len(current_text) > CHUNK_OVERLAP else current_text
                current_text = (tail + " " + seg).strip()
                current_paras = {pnum}
            else:
                current_text = (current_text + " " + seg).strip()
                current_paras.add(pnum)

    if len(current_text) >= MIN_CHUNK_TO_KEEP:
        chunks.append({"text": current_text, "paragraphs": sorted(list(current_paras))})
    return chunks


def embed_with_retries(client: OpenAI, text: str, retries: int = 3, backoff: float = 1.5) -> List[float]:
    """Small retry loop around embeddings to smooth transient network hiccups."""
    attempt = 0
    while True:
        try:
            resp = client.embeddings.create(input=text, model=EMBED_MODEL)
            return resp.data[0].embedding
        except Exception as e:
            attempt += 1
            if attempt > retries:
                raise
            time.sleep(backoff ** attempt)


# ---------- Main ----------
def process_all_docx_and_archive() -> bool:
    log("Starting DOCX batch processing and archive process...")
    log(f"SOP folder: {SOP_FOLDER}")
    archive_folder = os.path.join(SOP_FOLDER, ARCHIVE_SUBFOLDER)
    log(f"Archive folder: {archive_folder}")

    os.makedirs(archive_folder, exist_ok=True)

    docx_files = glob.glob(os.path.join(SOP_FOLDER, "*.docx"))
    log(f"Found {len(docx_files)} DOCX files")
    for docx in docx_files:
        log(f"  - {os.path.basename(docx)}")

    # Step/4: Initialize API clients (OpenAI via httpx + certifi; Pinecone new SDK)
    log("Step/4: Initializing API clients")

    # Force certifi CA bundle + explicit timeouts to avoid truststore/SSL hangs
    http_client = httpx.Client(verify=certifi.where(), timeout=HTTP_TIMEOUT_SECS)
    client = OpenAI(api_key=OPENAI_API_KEY, http_client=http_client)

    pc = Pinecone(api_key=PINECONE_API_KEY)

    # Step/5: Pinecone index check
    log("Step/5: Checking for Pinecone index")
    active_indexes = pc.list_indexes()  # list[str]
    log(f"All available indexes: {active_indexes}")

    if PINECONE_INDEX_NAME not in active_indexes:
        log(f"Index '{PINECONE_INDEX_NAME}' not found.")
        return False

    index = pc.Index(PINECONE_INDEX_NAME)
    log(f"Connected to index '{PINECONE_INDEX_NAME}'")

    # Step/6: Process files
    successful_docx, failed_docx = [], []

    for docx_count, docx_path in enumerate(docx_files, 1):
        docx_name = os.path.basename(docx_path)
        log(f"\nProcessing {docx_count}/{len(docx_files)}: {docx_name}")

        try:
            doc = Document(docx_path)
        except Exception as e:
            log(f"  ERROR loading DOCX: {e}")
            failed_docx.append(docx_name)
            continue

        paragraphs_content = []
        for i, p in enumerate(doc.paragraphs):
            t = (p.text or "").strip()
            if t:
                paragraphs_content.append({"paragraph_number": i + 1, "text": t})
        log(f"  Non-empty paragraphs: {len(paragraphs_content)}")

        chunks = chunk_paragraphs(paragraphs_content)
        log(f"  Total chunks: {len(chunks)}")

        total_upserted = 0
        for start in range(0, len(chunks), BATCH_SIZE):
            batch = chunks[start:start + BATCH_SIZE]
            vectors_to_upsert = []

            for offset, ch in enumerate(batch):
                idx = start + offset + 1
                text = ch["text"]
                para_refs = ch["paragraphs"]
                para_ref_str = format_paragraph_references(para_refs)

                try:
                    vector = embed_with_retries(client, text)
                except Exception as e:
                    log(f"    ERROR embedding chunk {idx}: {e}")
                    continue

                vec_id = f"docx-chunk-{uuid.uuid4()}"
                meta = {
                    "text": text,
                    "source": docx_name,
                    "chunk_index": idx - 1,
                    "doclink"
                    "paragraph_numbers": [str(n) for n in para_refs],
                    "paragraph_reference": para_ref_str,
                    
                }
                vectors_to_upsert.append((vec_id, vector, meta))
                log(f"    Prepared vector for chunk {idx} ({para_ref_str})")
                time.sleep(EMBED_DELAY_SEC)

            if vectors_to_upsert:
                try:
                    index.upsert(vectors=vectors_to_upsert)
                    total_upserted += len(vectors_to_upsert)
                    log(f"  Upserted {len(vectors_to_upsert)} vectors "
                        f"({total_upserted}/{len(chunks)})")
                except Exception as e:
                    log(f"  ERROR upserting batch: {e}")

            if start + BATCH_SIZE < len(chunks):
                time.sleep(BATCH_DELAY_SEC)

        if total_upserted > 0:
            try:
                dest = os.path.join(archive_folder, docx_name)
                shutil.move(docx_path, dest)
                successful_docx.append(docx_name)
                log("  Archived successfully")
            except Exception as e:
                log(f"  WARNING: Could not archive file: {e}")
        else:
            failed_docx.append(docx_name)

    # Summary
    log("\nFinal Summary")
    log(f"  Total DOCX files: {len(docx_files)}")
    log(f"  Success: {len(successful_docx)}")
    log(f"  Failed: {len(failed_docx)}")
    return True


if __name__ == "__main__":
    process_all_docx_and_archive()
