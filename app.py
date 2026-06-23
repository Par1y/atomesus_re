import argparse
import os
import warnings

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.requests import Request

warnings.filterwarnings("ignore")

parser = argparse.ArgumentParser()
parser.add_argument("--debug", action="store_true", help="Enable detailed request logging")
args, _ = parser.parse_known_args()
if args.debug:
    os.environ["CHAT2API_DEBUG"] = "true"

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

if args.debug:

    @app.middleware("http")
    async def debug_middleware(request, call_next):
        body_bytes = await request.body()
        body_text = body_bytes.decode("utf-8", errors="replace")[:1000]
        print(f"[DEBUG] {request.method} {request.url.path}")
        print(f"[DEBUG] Headers: {dict(request.headers)}")
        print(f"[DEBUG] Body: {body_text}")

        async def receive():
            return {"type": "http.request", "body": body_bytes}

        request = Request(request.scope, receive)
        response = await call_next(request)
        return response

from api.chat2api import router as chat2api_router

app.include_router(chat2api_router)

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=5005)
