import io
import json
import multiprocessing
import re
import time
import zipfile
from functools import partial
from pathlib import Path
from typing import List

import pandas as pd
import streamlit as st
from streamlit.runtime.uploaded_file_manager import UploadedFile

from snow_bench.utils import check_browser_available, SnowFlakeRunner, query_and_download

CACHE_PATH = Path(".snow")
CACHE_QUERY_RESULT_PATH = CACHE_PATH / "query_results"
CACHE_QUERY_RESULT_PATH.mkdir(parents=True, exist_ok=True)
CACHE_CONFIG = CACHE_PATH / "config.json"

st.session_state.cdp_endpoint = ""


def save_config_cache():
    cache = {
        "cdp_endpoint": st.session_state.cdp_endpoint,
    }
    CACHE_CONFIG.write_text(json.dumps(cache))


def load_config_cache():
    if CACHE_CONFIG.exists():
        return json.loads(CACHE_CONFIG.read_text())
    return dict()


def save_cdp_endpoint(endpoint):
    st.session_state.cdp_endpoint = endpoint
    save_config_cache()


def test_browser_available():
    if not re.match(r"^http://.*:\d+$", st.session_state.cdp_endpoint):
        st.toast(":orange-background[No CDP endpoint settings]", icon="⚠️")
        return
    with st.spinner("Checking browser..."):
        manager = multiprocessing.Manager()
        q = manager.dict()
        task = multiprocessing.Process(
            target=check_browser_available,
            args=(st.session_state.cdp_endpoint, q),
        )
        task.start()
        task.join()
        if q["browser_available"]:
            st.session_state.cdp_status = "Available"
        else:
            st.session_state.cdp_status = "Unavailable"
        st.session_state.snowflake_worksheet_page = q["snowflake_worksheet_page"]


@st.experimental_fragment
def browser_settings_frag():
    with st.container():
        with st.popover("Connect Browser"):
            input_endpoint = st.text_input("Set CDP endpoint")
            st.button(
                "Save", on_click=partial(save_cdp_endpoint, endpoint=input_endpoint)
            )
            cache = load_config_cache()
            cached_endpoint = cache.get("cdp_endpoint", "")
            if cached_endpoint:
                st.session_state.cdp_endpoint = cached_endpoint
        with st.expander(
                f"Your CDP endpoint: {st.session_state.cdp_endpoint}", expanded=True
        ):
            st.markdown(
                """
                **Browser status:** _{}_\n
                **Snowflake worksheet page:** _{}_
                """.format(
                    st.session_state.get("cdp_status", "Unknown"),
                    st.session_state.get("snowflake_worksheet_page", "Not Found"),
                )
            )
            st.button("Test connection", on_click=test_browser_available)


@st.experimental_fragment
def upload_query_frag():
    cntr = st.container()
    _uploaded_files: List[UploadedFile] = cntr.file_uploader(
        "Choose a SQL file", accept_multiple_files=True, type=["sql"]
    )
    st.session_state.uploaded_files = _uploaded_files


def get_queries():
    queries = []
    for uploaded_file in st.session_state.uploaded_files:
        content = uploaded_file.read().decode("utf8")
        sqls = [_ for _ in content.split(";") if _.strip()]
        queries.extend(sqls)
    return queries


def preview_checked(cntr, df):
    records = df.to_dict(orient="records")
    cntr.text("Preview")
    cntr.code(records, language="json")


@st.cache_data
def convert_to_zip(df):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "x", compression=zipfile.ZIP_DEFLATED) as zf:
        for _ in df.to_dict(orient="records"):
            if _["Select"] is False:
                continue
            file_path = CACHE_QUERY_RESULT_PATH / _["Result file"]
            zf.write(str(file_path), arcname=file_path.name)
    return buf.getvalue()


@st.experimental_fragment
def download_result_frag():
    preview_cntr = st.container()
    if not st.session_state.get("query_results"):
        return

    result_q = st.session_state.query_results
    files = [
        {"Select": True, "Result file": Path(y).name, "SQL": x}
        for x, y in result_q.items()
    ]
    data_df = pd.DataFrame(files)
    preview_cntr.text("Results")
    edited_df = preview_cntr.data_editor(
        data_df,
        column_config={
            "Select": st.column_config.CheckboxColumn(
                "Select", help="Select files to download", default=True, width="small"
            ),
            "Result file": st.column_config.TextColumn("Result file", width="medium"),
            "SQL": st.column_config.TextColumn("SQL", width="large"),
        },
        hide_index=False,
    )
    if not files:
        return

    pack_btn = preview_cntr.button("Pack files")
    if pack_btn:
        preview_cntr.download_button(
            label="Download zip",
            data=convert_to_zip(edited_df),
            file_name="Result.zip",
            mime="application/zip",
        )


def run_queries_action(cntr: st.container):
    st.session_state.query_results = dict()
    queries = get_queries()
    if not queries:
        st.toast(":orange-background[No SQLs uploaded!]", icon="⚠️")
        return

    cdp_endpoint = st.session_state.cdp_endpoint
    if not cdp_endpoint:
        st.toast(":orange-background[Please set CDP endpoint!]", icon="⚠️")

    manager = multiprocessing.Manager()
    result_q = manager.dict()
    snow_runner = SnowFlakeRunner(cdp_endpoint)

    placeholder = cntr.empty()
    with placeholder.container():
        with st.spinner("Running Query..."):
            time.sleep(0.5)
            for sql in queries:
                st.code(sql, language="sql")
                task = multiprocessing.Process(
                    target=query_and_download,
                    args=(snow_runner, sql, result_q),
                    kwargs={"result_path": CACHE_QUERY_RESULT_PATH},
                )
                task.start()
                task.join()
    snow_runner.stop()
    st.toast(":green-background[Done!]", icon="🎉")
    placeholder.empty()
    if not result_q:
        return
    st.session_state.query_results = dict(result_q)


def run_query_frag():
    cntr = st.container()
    st.button("Run", type="primary", on_click=partial(run_queries_action, cntr=cntr))


st.text("")
st.header("_snow-bench_ is :blue[cool] :sunglasses:", divider="rainbow")
st.subheader("Try _snow-bench_ for batch querying :green[Automation] on :blue[Snowflake].")
browser_settings_frag()
upload_query_frag()
run_query_frag()
download_result_frag()
