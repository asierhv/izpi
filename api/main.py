from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path
import os
import json

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "metadata" / "pools" / "pools_metadata"

@app.get("/pool/{address}")
def get_pool_metadata(address: str):
    file_path = DATA_DIR / f"{address}.json"
    
    # Debug information
    print(f"BASE_DIR: {BASE_DIR}")
    print(f"DATA_DIR: {DATA_DIR}")
    print(f"Looking for file: {file_path}")
    print(f"File exists: {file_path.exists()}")
    print(f"DATA_DIR exists: {DATA_DIR.exists()}")
    
    # List files in directory to verify
    if DATA_DIR.exists():
        files = list(DATA_DIR.glob("*.json"))
        print(f"Found {len(files)} JSON files in directory")
        if len(files) > 0:
            print(f"First few files: {[f.name for f in files[:5]]}")
    
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"Pool not found: {address}")
    
    with open(file_path, "r") as f:
        return json.load(f)