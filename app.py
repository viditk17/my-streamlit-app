import io
import os
import re
import shutil
import tempfile
from datetime import time
from typing import Optional, Tuple, List

import streamlit as st
import pandas as pd
from openpyxl import load_workbook
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
from openpyxl.formatting.rule import ColorScaleRule
import openai

# =============================================================================
# ✅ PROCESSOR (FULL BACKEND — UNCHANGED)
# =============================================================================

def process_edd_report(
    input_path: str,
    output_path: Optional[str] = None,
    source_sheet: str = "Query result",
    summary_sheet: str = "summary",
    apply_formatting: bool = True,
) -> str:

    if output_path is None:
        file_path = input_path
    else:
        shutil.copyfile(input_path, output_path)
        file_path = output_path

    df = pd.read_excel(file_path, sheet_name=source_sheet)
    df.columns = df.columns.str.strip()

    date_cols = ["EDD_Date", "PICKUP_CHLN_DATE", "Reached At Destination", "DLY_Date"]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    df = df.dropna(subset=["EDD_Date"])

    df["NEW_EDD_DATE"] = df["PICKUP_CHLN_DATE"] + pd.to_timedelta(
        df["TAT_DAYS"] - 1, unit="D"
    )

    df["ON_TIME_ARRIVAL"] = "No"
    valid_arrival = (
        df["Reached At Destination"].notna()
        & df["EDD_Date"].notna()
        & df["NEW_EDD_DATE"].notna()
    )

    cond1 = df["Reached At Destination"] <= df["NEW_EDD_DATE"]
    cond2 = (
        (df["Reached At Destination"].dt.date == df["EDD_Date"].dt.date)
        & (df["Reached At Destination"].dt.time < time(12, 0))
    )

    df.loc[valid_arrival & (cond1 | cond2), "ON_TIME_ARRIVAL"] = "Yes"

    df["ON_TIME_DELIVERY"] = "No"
    df.loc[
        df["DLY_Date"].notna()
        & df["EDD_Date"].notna()
        & (df["DLY_Date"] <= df["EDD_Date"]),
        "ON_TIME_DELIVERY",
    ] = "Yes"

    df["Week_Label"] = "W-" + df["EDD_Date"].dt.isocalendar().week.astype(str)

    weekly_total = df.groupby("Week_Label").size().reset_index(name="Picked Volume")
    zone_week = df.groupby(["Week_Label", "BKG_Zone"]).size().reset_index(name="Zone_CN")
    zone_week = zone_week.merge(weekly_total, on="Week_Label", how="left")
    zone_week["Zone_Percent"] = (zone_week["Zone_CN"] / zone_week["Picked Volume"] * 100).round(2)

    weeks = weekly_total["Week_Label"].tolist()
    zones = sorted(df["BKG_Zone"].dropna().unique())

    summary = pd.DataFrame(columns=weeks)
    summary.loc["Picked Volume"] = weekly_total.set_index("Week_Label")["Picked Volume"]

    for zone in zones:
        zdf = zone_week[zone_week["BKG_Zone"] == zone]
        summary.loc[f"Picked Vol. Zone {zone} %"] = {
            wk: (
                f"{r['Zone_Percent'].values[0]}% ({int(r['Zone_CN'].values[0])})"
                if not (r := zdf[zdf["Week_Label"] == wk]).empty
                else "0% (0)"
            )
            for wk in weeks
        }

    summary.index = summary.index.str.replace(r"__(.*)$", "", regex=True)

    with pd.ExcelWriter(
        file_path, engine="openpyxl", mode="a", if_sheet_exists="replace"
    ) as writer:
        summary.to_excel(writer, sheet_name=summary_sheet)

    wb = load_workbook(file_path)
    ws = wb[summary_sheet]
    ws.freeze_panes = "B2"
    wb.save(file_path)

    return file_path


# =============================================================================
# ✅ OPENAI (STABLE SDK)
# =============================================================================

def get_openai_key() -> Optional[str]:
    try:
        return st.secrets.get("OPENAI_API_KEY")
    except Exception:
        return os.getenv("OPENAI_API_KEY") or st.session_state.get("_openai_key")


def llm_answer(model: str, system: str, user: str) -> str:
    key = get_openai_key()
    if not key:
        return "❌ OpenAI API key missing."

    openai.api_key = key

    resp = openai.ChatCompletion.create(
        model=model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.2,
    )

    return resp.choices[0].message["content"].strip()


# =============================================================================
# ✅ STREAMLIT UI
# =============================================================================

st.set_page_config(page_title="EDD MIS Chatbot", layout="wide")
st.title("📦 EDD MIS Chatbot")

with st.sidebar:
    st.header("Settings")
    model = st.selectbox("Model", ["gpt-3.5-turbo"], index=0)
    key = st.text_input("OpenAI Key", type="password")
    if key:
        st.session_state["_openai_key"] = key

uploaded = st.file_uploader("Upload EDD Excel (.xlsx)", type=["xlsx"])

if uploaded:
    with tempfile.TemporaryDirectory() as td:
        in_path = os.path.join(td, "input.xlsx")
        with open(in_path, "wb") as f:
            f.write(uploaded.getvalue())

        out_path = process_edd_report(in_path)

        with open(out_path, "rb") as f:
            st.download_button(
                "⬇️ Download Processed Excel",
                f.read(),
                file_name="EDD_Report_Processed.xlsx",
            )

        q = st.text_input("Ask from summary:")
        if st.button("Ask") and q:
            ans = llm_answer(
                model=model,
                system="You are a logistics MIS analyst.",
                user=q,
            )
            st.success(ans)
