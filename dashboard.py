import streamlit as st
import pandas as pd
import json
import os
import asyncio
import nats
from datetime import datetime
from pathlib import Path

st.set_page_config(page_title="Command Center", layout="wide")

# CLEAN CSS
st.markdown("""
    <style>
    .stApp { background-color: #f4f4f4; }
    div.stButton > button:first-child { background-color: #d9534f; color: white; border: none; }
    .css-1r6slb0 { background-color: white; padding: 2rem; border-radius: 10px; }
    </style>
    """, unsafe_allow_html=True)

STATE_FILE = "data/system_state.json"
NATS_URL = "nats://localhost:4222"

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {}

def save_state(state):
    temp_file = STATE_FILE + ".tmp"
    with open(temp_file, "w") as f:
        json.dump(state, f, indent=2)
    os.replace(temp_file, STATE_FILE)

async def trigger_send(payload):
    nc = await nats.connect(NATS_URL)
    await nc.publish("reminder.due", json.dumps(payload).encode())
    await nc.close()

st.title("🛰️ Agent Dispatch Terminal")

state = load_state()
if not state:
    st.error("State file missing!")
    st.stop()

# Load Data
df = pd.DataFrame.from_dict(state, orient='index').reset_index().rename(columns={'index': 'txn_id'})

# Ensure necessary columns exist in the DataFrame
for col in ['mail_sent_at', 'replied_at', 'promised_date', 'reply_content']:
    if col not in df.columns: df[col] = None

# Sidebar - STRICT FILTERING
st.sidebar.header("🎯 Selection")
dist_list = sorted(df['distributor_id'].unique())
sel_dist = st.sidebar.selectbox("Distributor", dist_list)

# Filter retailers based on Distributor
retailer_df = df[df['distributor_id'] == sel_dist]
retailer_list = sorted(retailer_df['retailer_id'].unique())
sel_retailer = st.sidebar.selectbox("Retailer", retailer_list)

# Filter dates based on Retailer
date_df = retailer_df[retailer_df['retailer_id'] == sel_retailer]
date_list = sorted(date_df['transaction_date'].unique(), reverse=True)
sel_date = st.sidebar.selectbox("Transaction Date", date_list)

# FINAL FILTERED DATA - The source of truth for the table
final_df = date_df[date_df['transaction_date'] == sel_date].copy()

st.subheader(f"Transactions: {sel_retailer} | {sel_date}")

# Add selection column
final_df['Send'] = False

cols = ['Send', 'txn_id', 'sku_name', 'net_value', 'mail_status', 'mail_sent_at', 'reply_status', 'replied_at', 'promised_date', 'reply_content']

# Data Editor
edited_df = st.data_editor(
    final_df[cols],
    column_config={
        "Send": st.column_config.CheckboxColumn("Select", default=False),
        "mail_status": st.column_config.CheckboxColumn("Email Sent", disabled=True),
        "mail_sent_at": st.column_config.DatetimeColumn("Mail Sent Time", format="D MMM, h:mm a", disabled=True),
        "reply_status": st.column_config.CheckboxColumn("Replied", disabled=True),
        "replied_at": st.column_config.DatetimeColumn("Reply Time", format="D MMM, h:mm a", disabled=True),
        "promised_date": st.column_config.DateColumn("Pay Date", disabled=True),
        "net_value": st.column_config.NumberColumn("Amount", format="₹%.2f")
    },
    disabled=['txn_id', 'sku_name', 'net_value', 'mail_status', 'mail_sent_at', 'reply_status', 'reply_content', 'replied_at', 'promised_date'],
    hide_index=True,
    width="stretch",
    key="editor"
)

if st.button("🚀 APPROVE & SEND SELECTED"):
    to_send = edited_df[edited_df['Send'] == True]
    if to_send.empty:
        st.warning("Nothing selected.")
    else:
        sent_now = 0
        now_ts = datetime.utcnow().isoformat()
        for _, row in to_send.iterrows():
            tid = str(row['txn_id'])
            # Double check mail_status in state to prevent double sends
            if not state[tid].get('mail_status'):
                payload = {
                    "secondary_transaction_id": tid,
                    "retailer_id": sel_retailer,
                    "distributor_id": sel_dist
                }
                state[tid]['mail_status'] = True
                state[tid]['mail_sent_at'] = now_ts
                asyncio.run(trigger_send(payload))
                sent_now += 1
        
        save_state(state)
        st.success(f"Sent {sent_now} unique reminders.")
        st.rerun()

st.divider()
st.subheader("📥 Incoming Feed")
replies = df[df['reply_status'] == True].sort_values(by='replied_at', ascending=False)
if not replies.empty:
    # user needs: transaction_id, distributor_id, retailer_id, when the mail was sent, reply_content, date of reply
    feed_cols = ['txn_id', 'distributor_id', 'retailer_id', 'mail_sent_at', 'reply_content', 'replied_at']
    
    st.dataframe(
        replies[feed_cols], 
        column_config={
            "txn_id": "Transaction ID",
            "distributor_id": "Distributor ID",
            "retailer_id": "Retailer ID",
            "mail_sent_at": st.column_config.DatetimeColumn("When the mail was sent", format="D MMM YYYY, h:mm a"),
            "reply_content": "Reply content",
            "replied_at": st.column_config.DatetimeColumn("Date of reply", format="dddd, D MMM YYYY, h:mm a")
        },
        width="stretch",
        hide_index=True
    )
else:
    st.info("No replies detected yet.")

if st.sidebar.button("🔄 Sync Feed"):
    st.rerun()
