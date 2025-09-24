# app.py
from flask import Flask, request, render_template, jsonify, send_from_directory
from pinecone import Pinecone
from openai import OpenAI
from dotenv import load_dotenv
import os, re, json
from urllib.parse import quote, urlparse, urlunparse

# ==== CONFIG ====
INDEX_NAME = "hr-data"

# ================= Helpers =================

def _load_map_json():
    """Load optional filename->URL map JSON once per process."""
    path = os.getenv("DOCLINK_MAP_JSON", "").strip()
    if not path:
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            if isinstance(data, dict):
                return data
    except Exception as e:
        print(f"[DEBUG] Failed to load DOCLINK_MAP_JSON at '{path}': {e}")
    return {}

_DOCLINK_MAP = _load_map_json()
_DOC_BASE_URL = os.getenv("DOC_BASE_URL", "").rstrip("/")


def _derive_link_from_source(source_value: str) -> str | None:
    """
    Try to derive a link from metadata['source'] using:
      1) explicit filename->url mapping (DOCLINK_MAP_JSON)
      2) DOC_BASE_URL + URL-encoded filename
    """
    if not isinstance(source_value, str) or not source_value.strip():
        return None

    filename = source_value.strip()

    # 1) Mapping file takes precedence
    mapped = _DOCLINK_MAP.get(filename)
    if isinstance(mapped, str) and mapped.strip():
        return mapped.strip()

    # 2) Base URL fallback
    if _DOC_BASE_URL:
        encoded = quote(filename, safe="/()!-_.")
        return f"{_DOC_BASE_URL}/{encoded}"

    return None


def _normalize_url(u: str) -> str:
    """
    Normalize URLs for de-duplication:
    - lowercase scheme and netloc
    - strip default ports (:80 for http, :443 for https)
    - remove trailing slash in path (except root)
    - do NOT reorder query to avoid breaking signed URLs
    """
    try:
        p = urlparse(u)
        scheme = (p.scheme or "").lower()
        netloc = (p.netloc or "").lower()

        # Strip default ports
        if (scheme == "http" and netloc.endswith(":80")) or (scheme == "https" and netloc.endswith(":443")):
            netloc = netloc.rsplit(":", 1)[0]

        # Normalize path trailing slash (keep root '/')
        path = p.path or "/"
        if len(path) > 1 and path.endswith("/"):
            path = path.rstrip("/")

        return urlunparse((scheme, netloc, path, p.params, p.query, p.fragment))
    except Exception:
        # If parse fails, fallback to raw string
        return u.strip()


def _extract_doclink(md: dict, doc_id: int | None = None) -> str | None:
    """
    Extract a usable doc link from Pinecone metadata or fall back to:
      - filename->URL map
      - DOC_BASE_URL + encoded filename
      - URL sniffing inside 'text'
    Also logs when it fails to find anything.
    """
    if not md:
        print(f"[DEBUG] No metadata object for match {doc_id}.")
        return None

    # Primary keys (if ingestion later adds them)
    for k in ("doclink", "doc_link", "url", "link", "docUrl", "docURL"):
        v = md.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()

    # Derive from source filename if present
    source_val = md.get("source")
    link_from_source = _derive_link_from_source(source_val) if source_val else None
    if link_from_source:
        return link_from_source

    # Last resort: scan text for a URL
    txt = md.get("text", "") or ""
    m = re.search(r"https?://[^\s)>\]]+", txt)
    if m:
        return m.group(0)

    # Debug logging
    print(f"[DEBUG] No doclink found for match {doc_id}. Metadata keys: {list(md.keys())}")
    try:
        safe_md = {k: (v[:120] + "…") if isinstance(v, str) and len(v) > 120 else v
                   for k, v in md.items()}
        print("[DEBUG] Metadata content:", json.dumps(safe_md, indent=2))
    except Exception as e:
        print("[DEBUG] Could not log metadata:", e)
    return None

# ================= App Factory =================

def banda_ki_flask_app():
    load_dotenv()
    app = Flask(__name__, static_folder="static", static_url_path="/static")

    # Health for warmup probes
    @app.get("/health")
    def health():
        return "ok", 200

    # Explicit static route (optional – Flask already serves /static)
    @app.route("/static/<path:filename>")
    def static_files(filename):
        return send_from_directory(app.static_folder, filename)

    # Ensure folders exist in container (no-op locally if they do)
    def _validate_static_setup():
        cwd = os.getcwd()
        os.makedirs(os.path.join(cwd, "static"), exist_ok=True)
        os.makedirs(os.path.join(cwd, "templates"), exist_ok=True)

    def query_backend(query_text: str):
        pinecone_key = os.getenv("PINECONE_API_KEY")
        openai_key = os.getenv("OPENAI_API_KEY")
        if not pinecone_key or not openai_key:
            return {"error": "Missing API keys (set PINECONE_API_KEY and OPENAI_API_KEY)."}, None

        client = OpenAI(api_key=openai_key)
        pc = Pinecone(api_key=pinecone_key)

        # Ensure index exists
        names = [idx.name for idx in pc.list_indexes().indexes]
        if INDEX_NAME not in names:
            return {"error": f"Index '{INDEX_NAME}' not found. Available: {names}"}, None
        index = pc.Index(INDEX_NAME)

        # Embedding
        emb = client.embeddings.create(
            input=query_text,
            model="text-embedding-3-large"
        ).data[0].embedding

        # Vector search (need metadata for link derivation)
        res = index.query(vector=emb, top_k=5, include_metadata=True)
        matches = res.get("matches", []) if isinstance(res, dict) else getattr(res, "matches", [])

        docs = []
        context_parts = []
        seen = set()

        for i, m in enumerate(matches, 1):
            md = (m.get("metadata", {}) if isinstance(m, dict) else getattr(m, "metadata", {})) or {}
            link = _extract_doclink(md, i)

            # Keep text ONLY for LLM context
            txt = md.get("text", "") or ""
            if txt:
                context_parts.append(txt)

            # Capture human-friendly display name (metadata['source'])
            source_name = (md.get("source") or "").strip() or None

            if link:
                norm = _normalize_url(link)
                if norm in seen:
                    continue
                seen.add(norm)
                score = round((m.get("score", 0.0) if isinstance(m, dict) else getattr(m, "score", 0.0)) or 0.0, 4)
                docs.append({
                    "doc_id": i,
                    "doclink": link,
                    "source": source_name,
                    "similarity_score": score
                })

        # Hard-cap to 5 unique links
        docs = docs[:5]
        context = "\n\n---\n\n".join(context_parts) if context_parts else "No relevant document text provided."

        # LLM answer
        chat = client.chat.completions.create(
            model="gpt-4o",
            temperature=0,
            messages=[
                {"role": "system",
                 "content": "You are a helpful assistant. Answer only from the provided company documents."},
                {"role": "user",
                 "content": (
                     "Context from standard operating procedure documents:\n\n"
                     f"{context}\n\n"
                     "Based only on the information above, answer directly and cite the documents. "
                     "If not clearly answered in the docs, say so. Reject personal questions. "
                     f"Question: {query_text}"
                 )}
            ],
        )
        answer = chat.choices[0].message.content if chat.choices else ""
        return {"answer": answer, "documents": docs, "total_documents": len(docs)}, None

    @app.route("/")
    def home():
        # Must have /home/site/wwwroot/templates/index.html in Azure
        return render_template("index.html")

    @app.route("/query", methods=["POST"])
    def query_hr():
        data = request.get_json(force=True) or {}
        q = (data.get("query") or "").strip()
        if not q:
            return jsonify({"error": "Query cannot be empty"}), 400
        resp, _ = query_backend(q)
        code = 200 if "error" not in resp else 500
        return jsonify(resp), code

    _validate_static_setup()
    return app

# Expose module-level 'app' for Gunicorn
app = banda_ki_flask_app()

# Local dev only (Azure will NOT run this because gunicorn imports app:app)
if __name__ == "__main__":
    # Use 8000 locally if you want to mirror Azure's WEBSITES_PORT
    app.run(host="0.0.0.0", port=8000, debug=True)
