import os
import re
import subprocess
import tempfile
import time
import uuid
from pathlib import Path
from typing import overload, Union

import pandas as pd
from playwright.sync_api import sync_playwright, Page, Locator

# install browser plugins
subprocess.check_call(["playwright", "install"])

RUN_QUERY_WAIT_TIME = 5 * 60 * 1000


class SnowFlakeRunner:
    def __init__(self, cdp_endpoint="http://localhost:9222/"):
        self.cdp_endpoint = cdp_endpoint
        self.playwright = None
        self.browser = None

    def __enter__(self):
        self.start()
        return self

    def search_workbench_page(self) -> Page:
        if not self.browser:
            raise ValueError("Browser is not initialized")

        for page in self.browser.contexts[0].pages:
            if re.match(r"^https://app\.snowflake\.com/.*query$", page.url):
                return page
        raise ValueError("No snowflake page found")

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def start(self):
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.connect_over_cdp(self.cdp_endpoint)

    def stop(self):
        if self.playwright:
            self.playwright.stop()

    def input_query(self, page: Page, sql_text):
        worksheet: Locator = page.locator('[aria-label="worksheet"]')
        worksheet.fill(sql_text)

    def run_query(self, page: Page):
        btn: Locator = page.locator('[aria-label="Run"][role="button"]')
        btn.click()

    def get_query_rows_count(self, page: Page) -> int:
        page.wait_for_selector('[role="listitem"]', timeout=RUN_QUERY_WAIT_TIME)
        time.sleep(1)
        details = page.get_by_role("listitem").all()
        if len(details) > 1:
            row = details[1]
            row = row.text_content().replace("Rows", "")
            return int(row)
        raise ValueError(f"Cannot parse query details, {details}")

    @overload
    def download_result(self, page: Page, filename: str):
        ...

    @overload
    def download_result(self, page: Page) -> pd.DataFrame:
        ...

    def download_result(
            self, page: Page, filename: Union[str, None] = None
    ) -> Union[pd.DataFrame, None]:
        rows = self.get_query_rows_count(page)
        if rows == 0:
            return
        btn: Locator = page.locator('[data-testid="result-download-menu-button"]')
        btn.click()
        time.sleep(1)
        with page.expect_download() as download_info:
            # Perform the action that initiates download
            page.get_by_text("Download as .csv", exact=True).click(
                button="left", force=True
            )
        download = download_info.value
        download_path = download.path()
        if not os.path.exists(download_path):
            raise FileNotFoundError(download_path)
        if filename:
            download.save_as(filename)
            Path(download_path).replace(filename)
        else:
            temp_filename = tempfile.mktemp(suffix=".csv", prefix="result-", dir="..")
            download.save_as(temp_filename)
            if not os.path.exists(temp_filename):
                raise FileNotFoundError(temp_filename)
            df = pd.read_csv(temp_filename)
            return df

    def is_active(self):
        if self.playwright is not None and self.browser.contexts[0].pages:
            return True
        return False


def query_and_download(snow_runner: SnowFlakeRunner, sql_text, q, result_path=None):
    if snow_runner.playwright is None:
        snow_runner.start()
    page = snow_runner.search_workbench_page()
    snow_runner.input_query(page, sql_text)
    snow_runner.run_query(page)
    if result_path:
        filename = os.path.join(result_path, f"result-{str(uuid.uuid4())[:8]}.csv")
    else:
        filename = tempfile.mktemp(suffix=".csv", prefix="result-", dir="..")
    snow_runner.download_result(page, filename)
    q[sql_text] = filename


def query(cdp_endpoint, sql_text, q):
    with SnowFlakeRunner(cdp_endpoint) as snow:
        page = snow.search_workbench_page()
        snow.input_query(page, sql_text)
        snow.run_query(page)
        df = snow.download_result(page)
        q[sql_text] = df


def check_browser_available(cdp_endpoint, q):
    q["browser_available"] = False
    q["snowflake_worksheet_page"] = "Not Found"
    try:
        with SnowFlakeRunner(cdp_endpoint) as snow:
            if snow.is_active():
                q["browser_available"] = True
                q["snowflake_worksheet_page"] = snow.search_workbench_page().url
    except Exception:
        pass
