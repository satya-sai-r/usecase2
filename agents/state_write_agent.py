import asyncio
import json
import logging
import os
from pathlib import Path
import pandas as pd
from datetime import datetime
import tempfile
import time
import threading

import nats
import psycopg
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [state_write] %(levelname)s %(message)s",
    handlers=[logging.FileHandler("logs/state_write.log"), logging.StreamHandler()],
)
log = logging.getLogger(__name__)

STATE_FILE = "data/system_state.json"
NATS_URL   = os.getenv("NATS_URL")
DSN        = os.getenv("POSTGRES_DSN")
OUTPUTS    = Path("outputs")

# Cross-platform file locking
_file_lock = threading.Lock()

def update_json_state(txn_id, raw_body=None, received_at=None, promised_date=None, mail_sent_at=None):
    txn_id = str(txn_id)
    if not os.path.exists(STATE_FILE): 
        log.error(f"State file missing: {STATE_FILE}")
        return
    
    # Use file locking to prevent race conditions
    with _file_lock:
        try:
            with open(STATE_FILE, "r") as f:
                state = json.load(f)
            
            if txn_id in state:
                if raw_body is not None:
                    state[txn_id]['reply_status'] = True
                    state[txn_id]['reply_content'] = raw_body
                if received_at is not None:
                    state[txn_id]['replied_at'] = received_at
                if promised_date is not None:
                    state[txn_id]['promised_date'] = promised_date
                if mail_sent_at is not None:
                    state[txn_id]['mail_status'] = True
                    state[txn_id]['mail_sent_at'] = mail_sent_at
                
                # Write updated state atomically
                temp_fd, temp_path = tempfile.mkstemp(dir=os.path.dirname(STATE_FILE))
                try:
                    with os.fdopen(temp_fd, "w") as temp_f:
                        json.dump(state, temp_f, indent=2)
                    os.replace(temp_path, STATE_FILE)
                    log.info(f"JSON state updated for Txn: {txn_id}")
                except Exception as e:
                    os.unlink(temp_path)
                    raise e
            else:
                log.warning(f"Txn {txn_id} not found in JSON state during update")
        except Exception as e:
            log.error(f"Failed to update JSON state for Txn {txn_id}: {e}")
            raise

async def update_db_sent(txn_id, sent_at, db):
    try:
        # First verify transaction exists
        result = await db.execute("""
            SELECT COUNT(*) as count FROM transactions 
            WHERE secondary_transaction_id = %s
        """, (txn_id,))
        count = await result.fetchone()
        
        if count[0] == 0:
            log.warning(f"Transaction {txn_id} not found in database - skipping update")
            return
        
        # Update the transaction
        await db.execute("""
            UPDATE transactions 
            SET reminder_sent_at = %s, reminder_count = reminder_count + 1
            WHERE secondary_transaction_id = %s
        """, (sent_at, txn_id))
        await db.commit()
        log.info(f"Database updated for Txn: {txn_id}")
    except Exception as e:
        log.error(f"DB update sent error: {e}")
        await db.rollback()

def append_to_excel(data: dict):
    dist_id = data.get("distributor_id", "Unknown")
    excel_path = OUTPUTS / f"{dist_id}_replies.xlsx"
    OUTPUTS.mkdir(exist_ok=True)

    new_row = {
        "retailer_id": data.get("retailer_id"),
        "distributor_id": dist_id,
        "transaction_id": data.get("transaction_id"),
        "reply_received_at": data.get("received_at"),
        "promised_date": data.get("date"),
        "promised_days": data.get("days"),
        "amount_confirmed": data.get("amount"),
        "raw_reply": data.get("raw_reply")
    }

    if excel_path.exists():
        df = pd.read_excel(excel_path)
        df = pd.concat([df, pd.DataFrame([new_row])], ignore_index=True)
    else:
        df = pd.DataFrame([new_row])
    
    df.to_excel(excel_path, index=False)
    log.info(f"Excel updated: {excel_path}")

async def main():
    nc  = await nats.connect(NATS_URL)
    
    # Listen for parsed replies
    sub_parsed = await nc.subscribe("reply.parsed")
    # Listen for sent reminders
    sub_sent = await nc.subscribe("reminder.sent")
    
    log.info("State Write Agent (v2.2) running...")

    async with await psycopg.AsyncConnection.connect(DSN) as db:
        async def watch_parsed():
            async for msg in sub_parsed.messages:
                try:
                    data = json.loads(msg.data)
                    txn_id = data.get("transaction_id")
                    update_json_state(
                        txn_id, 
                        raw_body=data.get("raw_reply"), 
                        received_at=data.get("received_at"),
                        promised_date=data.get("date")
                    )
                    append_to_excel(data)
                except Exception as e:
                    log.error(f"State Parsed Error: {e}")

        async def watch_sent():
            async for msg in sub_sent.messages:
                try:
                    data = json.loads(msg.data)
                    txn_id = data.get("transaction_id")
                    sent_at = data.get("sent_at")
                    update_json_state(txn_id, mail_sent_at=sent_at)
                    await update_db_sent(txn_id, sent_at, db)
                except Exception as e:
                    log.error(f"State Sent Error: {e}")

        await asyncio.gather(watch_parsed(), watch_sent())

if __name__ == "__main__":
    import selectors
    loop_factory = lambda: asyncio.SelectorEventLoop(selectors.SelectSelector())
    asyncio.run(main(), loop_factory=loop_factory)
