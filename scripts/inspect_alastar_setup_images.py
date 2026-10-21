"""Fetch only the four identified public chart images for one-off visual review."""
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import urlsplit

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config.settings import settings
from core.utils.proxy_env import ensure_proxy_env


def main():
    root = Path(__file__).resolve().parents[1]
    out = root / "data/research/alastar_response_20261021"
    images = out / "images"
    images.mkdir(exist_ok=True)
    ensure_proxy_env(settings)
    inventory = json.loads((out / "image_inventory.json").read_text(encoding="utf-8"))
    receipt = []
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=Retry(total=2, backoff_factor=.5)))
    for item in inventory:
        urls = re.findall(r"https?://[^\x27\x22)]+", item["style"])
        assert len(urls) == 1
        u = urlsplit(urls[0])
        assert u.scheme == "https" and re.fullmatch(r"cdn[0-9]+\.telesco\.pe", u.hostname or "")
        target = images / f"{item['id']}.jpg"
        if target.exists():
            raw = target.read_bytes()
            receipt.append({"message_id": item["id"], "path": str(target.relative_to(root)),
                            "host": u.hostname, "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw),
                            "version_limitation": "Current public archived preview asset; publication-time image version unknown."})
            continue
        with session.get(urls[0], timeout=(10, 30), stream=True) as response:
            response.raise_for_status()
            assert urlsplit(response.url).hostname == u.hostname
            assert response.headers.get("content-type", "").startswith("image/")
            chunks, size = [], 0
            for chunk in response.iter_content(65536):
                size += len(chunk)
                if size > 5*1024*1024: raise RuntimeError("Public chart image exceeds bound")
                chunks.append(chunk)
        raw = b"".join(chunks)
        target.write_bytes(raw)
        receipt.append({"message_id": item["id"], "path": str(target.relative_to(root)),
                        "host": u.hostname, "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw),
                        "version_limitation": "Current public archived preview asset; original publication-time image version unknown."})
        print(item["id"], len(raw), flush=True)
    (out / "image_download_receipt.json").write_text(json.dumps(receipt, indent=2), encoding="utf-8")


if __name__ == "__main__": main()
