import asyncio
import os
from fastapi import FastAPI, HTTPException
from models import WebhookPayload
from db import init_pool, close_pool, insert_data   # ✅ fixed import (removed enqueue_payload)

app = FastAPI()

# -------------------------------
# Background worker setup
# -------------------------------
QUEUE = asyncio.Queue(maxsize=10000)  # ✅ adjustable depending on load
WORKER_TASK = None


async def worker():
    """Continuously process payloads from queue and insert into DB"""
    while True:
        payload = await QUEUE.get()
        try:
            # Insert in background thread (to avoid blocking event loop)
            await asyncio.to_thread(insert_data, WebhookPayload(**payload))
        except Exception as e:
            print(f"❌ Worker failed for payload: {e}")
        finally:
            QUEUE.task_done()


def enqueue_payload(payload: dict):
    """Non-blocking enqueue of payload"""
    try:
        QUEUE.put_nowait(payload)
    except asyncio.QueueFull:
        raise


# -------------------------------
# FastAPI lifecycle
# -------------------------------
@app.on_event("startup")
async def startup():
    global WORKER_TASK
    await asyncio.to_thread(init_pool)   # ✅ run pool init in background thread (sync → async safe)
    WORKER_TASK = asyncio.create_task(worker())
    print("✅ Worker started and DB pool ready.")


@app.on_event("shutdown")
async def shutdown():
    global WORKER_TASK
    if WORKER_TASK:
        WORKER_TASK.cancel()
    await asyncio.to_thread(close_pool)   # ✅ safe close
    print("✅ Worker stopped and DB pool closed.")


# -------------------------------
# Routes
# -------------------------------
@app.get("/")
async def root():
    return {"message": "Webhook live. POST /webhook"}


@app.post("/webhook")
async def webhook_handler(payload: WebhookPayload):
    try:
        enqueue_payload(payload.dict())  # ✅ enqueue quickly
        return {"status": "accepted"}
    except asyncio.QueueFull:
        raise HTTPException(status_code=503, detail="Ingestion queue full, try again later.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
