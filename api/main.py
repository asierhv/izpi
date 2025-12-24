from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import os
import json
import Path

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # O limita a ["http://localhost:8000"] si quieres seguridad
    allow_methods=["*"],
    allow_headers=["*"],
)
BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "metadata/pools/pools_metadata"
 
@app.get("/pool/{address}")
def get_pool_metadata(address: str):
    file_path = f"{DATA_DIR}/{address}.json"
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail=f"Pool not found")
    with open(file_path, "r") as f:
        return json.load(f)
