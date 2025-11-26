import asyncio
import os
import logging

from fastapi import FastAPI, HTTPException, Request

from models import WebhookPayload
from db import init_pool, close_pool, insert_data, save_failed_payload

app = FastAPI()

# -------------------------------
# Background worker setup
# -------------------------------
MAX_QUEUE = int(os.getenv("MAX_QUEUE", "10000"))  # from .env :contentReference[oaicite:3]{index=3}
QUEUE: asyncio.Queue = asyncio.Queue(maxsize=MAX_QUEUE)
WORKER_TASK: asyncio.Task | None = None


async def worker():
    """Continuously process payloads from queue and insert into DB."""
    logging.info("👷 Worker loop started.")
    while True:
        raw_payload = await QUEUE.get()
        try:
            # 1) Validate + normalize via Pydantic
            try:
                parsed = WebhookPayload(**raw_payload)
            except Exception as e:
                logging.exception("❌ Validation failed for payload.")
                # Store raw payload for inspection/retry
                await asyncio.to_thread(
                    save_failed_payload,
                    raw_payload,
                    f"Validation error: {e}",
                )
                continue

            # 2) Insert into DB (sync → async safe via to_thread)
            try:
                await asyncio.to_thread(insert_data, parsed)
            except Exception as e:
                logging.exception("❌ Insert failed for payload.")
                await asyncio.to_thread(
                    save_failed_payload,
                    raw_payload,
                    f"DB insert error: {e}",
                )

        finally:
            QUEUE.task_done()


def enqueue_payload(payload: dict):
    """Non-blocking enqueue of payload."""
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
    logging.info("🚀 Starting up service...")
    await asyncio.to_thread(init_pool)
    WORKER_TASK = asyncio.create_task(worker())
    logging.info("✅ Worker started and DB pool ready.")


@app.on_event("shutdown")
async def shutdown():
    global WORKER_TASK
    logging.info("🛑 Shutting down service...")
    if WORKER_TASK:
        WORKER_TASK.cancel()
        try:
            await WORKER_TASK
        except asyncio.CancelledError:
            pass
    await asyncio.to_thread(close_pool)
    logging.info("✅ Worker stopped and DB pool closed.")


# -------------------------------
# Routes
# -------------------------------
@app.get("/")
async def root():
    return {"message": "Webhook live. POST /webhook"}


@app.post("/webhook")
async def webhook_handler(request: Request):
    """
    Accept raw JSON from Petpooja, quickly enqueue it, and return 200.
    No Pydantic validation at this stage => 0% 422 errors.
    """
    try:
        payload = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    if not isinstance(payload, dict):
        # Petpooja always sends an object, not array
        raise HTTPException(status_code=400, detail="Body must be a JSON object")

    try:
        enqueue_payload(payload)
        return {"status": "accepted"}
    except asyncio.QueueFull:
        raise HTTPException(
            status_code=503,
            detail="Ingestion queue full, try again later.",
        )
    except Exception as e:
        logging.exception("Unexpected error in webhook_handler")
        raise HTTPException(status_code=500, detail=str(e))
