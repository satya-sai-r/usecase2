import asyncio
import email
import email.header
import email.utils
import imaplib
import json
import logging
import os
import re
import nats
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [reply_monitor] %(levelname)s %(message)s",
    handlers=[logging.FileHandler("logs/reply_monitor.log"), logging.StreamHandler()],
)
log = logging.getLogger(__name__)

IMAP_HOST = os.getenv("IMAP_HOST")
IMAP_USER = os.getenv("IMAP_USER")
IMAP_PASS = os.getenv("IMAP_PASS")
NATS_URL  = os.getenv("NATS_URL")

def extract_id(subject):
    match = re.search(r"\[(.*?)\]", subject)
    if match:
        return match.group(1).strip().split('.')[0]
    return None

def poll_imap(queue: list):
    try:
        log.info(f"Connecting to IMAP {IMAP_HOST}...")
        mail = imaplib.IMAP4_SSL(IMAP_HOST)
        mail.login(IMAP_USER, IMAP_PASS)
        mail.select("INBOX")
        
        _, msg_ids = mail.search(None, '(SUBJECT "Payment Reminder")')
        id_list = msg_ids[0].split()
        log.info(f"Found {len(id_list)} messages matching 'Payment Reminder'")
        
        for msg_id in id_list:
            _, data = mail.fetch(msg_id, "(BODY.PEEK[])")
            raw_email = data[0][1]
            msg = email.message_from_bytes(raw_email)
            
            # 1. Extract and Decode Subject
            subject_raw = msg.get("Subject")
            if not subject_raw: continue
            decoded_parts = email.header.decode_header(subject_raw)
            subject = ""
            for part, encoding in decoded_parts:
                if isinstance(part, bytes):
                    subject += part.decode(encoding or "utf-8", errors="replace")
                else:
                    subject += part
            
            txn_id = extract_id(subject)
            if not txn_id: continue

            # 2. Extract ACTUAL Reply Date from Email Headers
            # This captures the "Tue, 7 Apr 2026 at 13:54" information
            date_str = msg.get("Date")
            if date_str:
                parsed_date = email.utils.parsedate_to_datetime(date_str)
                # Convert to ISO format for JSON
                received_at = parsed_date.isoformat()
            else:
                received_at = datetime.utcnow().isoformat()
            
            # 3. Extract and Clean Body (Reply Content)
            body = ""
            if msg.is_multipart():
                for part in msg.walk():
                    if part.get_content_type() == "text/plain":
                        payload = part.get_payload(decode=True)
                        if payload: body = payload.decode(errors="replace")
                        break
            else:
                payload = msg.get_payload(decode=True)
                if payload: body = payload.decode(errors="replace")
            
            # CLEANING: Keep only the new content, stop at "On ... wrote:" or other markers
            lines = body.splitlines()
            clean_lines = []
            
            # Common patterns for attribution headers
            attribution_patterns = [
                r"^On\s+.*wrote:\s*$",
                r"^-+Original Message-+$",
                r"^From:\s+.*",
                r"^Sent:\s+.*",
                r"^To:\s+.*",
                r"^Subject:\s+.*"
            ]
            
            for i, line in enumerate(lines):
                stripped = line.strip()
                if not stripped: continue
                
                # Check for start of attribution block
                is_attribution = False
                for pattern in attribution_patterns:
                    if re.match(pattern, stripped, re.IGNORECASE):
                        is_attribution = True
                        break
                
                # Multi-line attribution check: line starts with "On " and some subsequent line has "wrote:"
                if not is_attribution and stripped.startswith("On "):
                    # Look ahead a few lines for "wrote:"
                    for j in range(i, min(i + 5, len(lines))):
                        if "wrote:" in lines[j].lower():
                            is_attribution = True
                            break
                
                if is_attribution:
                    break
                
                # Extract content, stripping leading '>' but preserving the message
                content = stripped.lstrip(">").strip()
                if content:
                    clean_lines.append(content)
            
            clean_body = " ".join(clean_lines)[:500]
            
            queue.append({
                "transaction_id": txn_id,
                "body": clean_body,
                "received_at": received_at
            })
            log.info(f"CAPTURED: Txn {txn_id} | Sent at: {received_at}")
        
        mail.logout()
    except Exception as e:
        log.error(f"IMAP Error: {e}")

async def main():
    nc = await nats.connect(NATS_URL)
    log.info("Reply Monitor v5.1 (Header-based Date) active...")
    while True:
        queue = []
        await asyncio.get_event_loop().run_in_executor(None, poll_imap, queue)
        for item in queue:
            await nc.publish("reply.received", json.dumps(item).encode())
        await asyncio.sleep(15)

if __name__ == "__main__":
    asyncio.run(main())
