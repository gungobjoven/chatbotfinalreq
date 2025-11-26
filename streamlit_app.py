import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from snowflake.snowpark import Session
import google.generativeai as genai

# ---------------------------------------------------
# 1. CONFIGURE GEMINI API
# ---------------------------------------------------
genai.configure(api_key="AIzaSyANtNJodyCXiALBavNki7XSuh9bn6RB_JA")
MODEL_NAME = "models/gemini-2.5-flash"
gemini_model = genai.GenerativeModel(MODEL_NAME)

# ---------------------------------------------------
# 2. CONFIGURE SNOWFLAKE
# ---------------------------------------------------
connection_parameters = {
    "account": st.secrets["snowflake"]["account"],
    "user": st.secrets["snowflake"]["user"],
    "password": st.secrets["snowflake"]["password"],
    "role": "ACCOUNTADMIN",
    "warehouse": st.secrets["snowflake"]["warehouse"],
    "database": "CHATBOT_DB",
    "schema": "CHATBOT_SCHEMA",
}

@st.cache_resource
def get_session():
    return Session.builder.configs(connection_parameters).create()

session = get_session()

# ---------------------------------------------------
# STREAMLIT PAGE SETTINGS
# ---------------------------------------------------
st.set_page_config(page_title="AI + Stock Data Chatbot", layout="wide")
st.title("🤖 AI Chatbot + 📊 Tata Motors Stock Assistant")

# ---------------------------------------------------
# 3. CHAT HISTORY
# ---------------------------------------------------
if "messages" not in st.session_state:
    st.session_state["messages"] = []

# Display previous messages
for msg in st.session_state["messages"]:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])

# ---------------------------------------------------
# 4. Helper Functions
# ---------------------------------------------------
def run_sql(query):
    """Runs SQL and returns pandas dataframe"""
    try:
        df = session.sql(query).to_pandas()
        df.columns = [c.lower() for c in df.columns]   # normalize
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    except Exception as e:
        st.error(f"SQL Error: {e}")
        return pd.DataFrame()

def detect_dataset_question(q):
    q = q.lower()

    # detect "last N rows"
    if "last" in q and "rows" in q:
        import re
        m = re.search(r'last\s+(\d+)\s+rows', q)
        if m:
            return ("last_n_rows", int(m.group(1)))

    # detect plot requests
    if "plot" in q or "graph" in q:
        columns_available = [
            "open", "high", "low", "close", "volume",
            "rsi_1", "rsi_2",
            "macd", "macd_signal", "macd_hist",
            "ema_50", "ema_200",
            "52_week_high", "distance_from_high"
        ]

        cols = [col for col in columns_available if col in q]
        if not cols:
            cols = ["close"]  # default

        return ("plot_columns", cols)

    return None  # means it's a Gemini question

# ---------------------------------------------------
# 5. Chat Input
# ---------------------------------------------------
if user_input := st.chat_input("Ask anything… dataset questions or normal questions"):
    
    # Record user message
    st.chat_message("user").markdown(user_input)
    st.session_state["messages"].append({"role": "user", "content": user_input})

    # Check if question is related to dataset
    intent = detect_dataset_question(user_input)

    # ---------------------------------------------------
    # CASE 1: DATASET QUESTION → Snowflake logic
    # ---------------------------------------------------
    if intent:
        kind, payload = intent

        if kind == "last_n_rows":
            n = payload
            sql = f"SELECT * FROM TATA_MOTORS_DATA ORDER BY timestamp DESC LIMIT {n}"
            df = run_sql(sql)

            if not df.empty:
                st.chat_message("assistant").markdown(f"Here are the last **{n} rows**:")
                st.dataframe(df)
                st.session_state["messages"].append({"role": "assistant", "content": f"Displayed last {n} rows."})

        elif kind == "plot_columns":
            cols = payload
            sql = "SELECT timestamp, " + ", ".join(cols) + " FROM TATA_MOTORS_DATA ORDER BY timestamp"
            df = run_sql(sql)

            if not df.empty:
                st.chat_message("assistant").markdown(f"📊 Plotting: **{', '.join(cols)}**")
                st.dataframe(df)

                for col in cols:
                    if col in df.columns:
                        fig, ax = plt.subplots(figsize=(10, 4))
                        ax.plot(df['timestamp'], df[col])
                        ax.set_title(col)
                        ax.set_xlabel("Timestamp")
                        ax.set_ylabel(col)
                        st.pyplot(fig)

                st.session_state["messages"].append({"role": "assistant", "content": "Generated requested plots."})

    # ---------------------------------------------------
    # CASE 2: NORMAL QUESTION → Gemini API response
    # ---------------------------------------------------
    else:
        try:
            response = gemini_model.generate_content(user_input)
            bot_reply = response.text
        except Exception as e:
            bot_reply = f"Error using Gemini API: {e}"

        st.chat_message("assistant").markdown(bot_reply)
        st.session_state["messages"].append({"role": "assistant", "content": bot_reply})
