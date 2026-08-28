#!/usr/bin/env python3
"""
Payments API - REST API for payment data
"""
import os
import logging
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Payments API",
    description="API for accessing payment data",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {
        "service": "Payments API",
        "version": "1.0.0",
        "endpoints": {
            "health": "/health",
            "payments": "/api/v1/payments"
        }
    }

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.get("/api/v1/payments")
async def get_payments():
    return {"message": "Payment data endpoint", "payments": []}

if __name__ == "__main__":
    port = int(os.getenv("API_PORT", 7001))
    uvicorn.run("api:app", host="0.0.0.0", port=port, reload=True)
