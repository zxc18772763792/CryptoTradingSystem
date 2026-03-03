from __future__ import annotations

import os

import uvicorn


if __name__ == "__main__":
    host = str(os.getenv("OPS_HOST") or "127.0.0.1")
    port = int(os.getenv("OPS_PORT") or "8711")
    uvicorn.run("core.ops.service.api:app", host=host, port=port, reload=False)
