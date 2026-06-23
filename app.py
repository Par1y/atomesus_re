import warnings

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

warnings.filterwarnings("ignore")

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from api.chat2api import router as chat2api_router

app.include_router(chat2api_router)

if __name__ == "__main__":
    uvicorn.run("app:app", host="0.0.0.0", port=5005)
