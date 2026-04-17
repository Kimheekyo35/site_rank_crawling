import logging
import os
import re
from datetime import datetime
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from zoneinfo import ZoneInfo

import psycopg2
from dotenv import load_dotenv
from playwright.sync_api import Browser, BrowserContext, Error as PlaywrightError
from playwright.sync_api import Page, Playwright, TimeoutError as PlaywrightTimeoutError, sync_playwright
from psycopg2 import sql
from psycopg2.extras import execute_values

load_dotenv(override=True)

logger = logging.getLogger(__name__)
if not logger.handlers:
    logging.basicConfig(
        level=os.getenv("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


def _getenv_str(name: str, default: str = "") -> str:
    value = os.getenv(name)
    return value.strip() if value is not None else default


def _getenv_int(name: str, default: int) -> int:
    raw = _getenv_str(name)
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid integer for %s=%r; using default %s.", name, raw, default)
        return default


def _getenv_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}

SKINCARE_URL = "https://www.yesstyle.com/en/beauty-skin-care/list.html/bcc.15544_bpt.46"
NUMBUZIN_URL = "https://www.yesstyle.com/en/numbuzin/list.html/bpt.299_bid.326359"

SCHEMA_NAME = "benow"
TABLE_NAME = "yesstyle_table"
TARGET_COUNT = 100
SEOUL_TZ = ZoneInfo("Asia/Seoul")

COL_RANK = "순위"
COL_BRAND = "브랜드명"
COL_PRODUCT = "제품명"
COL_LIST_PRICE = "정가"
COL_PRICE = "가격"
COL_SALES = "판매량"
COL_STORE = "판매샵"
COL_COLLECTED_AT = "날짜와시간"
COL_CHANNEL = "채널"
COL_CATEGORY = "카테고리"
COL_SPECIAL = "오특유무"
COL_COUNTRY = "국가"

COUNTRY_BUTTON_SELECTOR = (
    "body > header > div.header-module-scss-module__lXl03a__mainHeaderToolWrapper.MuiBox-root.mui-0 "
    "> div > div.header-module-scss-module__lXl03a__toolsLeft.MuiBox-root.mui-0 > button:nth-child(1)"
)
ITEM_SELECTOR = "a[class*='itemContainer']"

DB_HOST = _getenv_str("PG_HOST")
DB_PORT = _getenv_str("PG_PORT")
DB_DATABASE = _getenv_str("BENOW_DATABASE", "benow_db")
DB_USER = _getenv_str("PG_USER")
DB_PASSWORD = _getenv_str("PG_PASSWORD")
HEADLESS = _getenv_bool("PLAYWRIGHT_HEADLESS", True)
BROWSER_WINDOW_WIDTH = _getenv_int("BROWSER_WINDOW_WIDTH", 1440)
BROWSER_WINDOW_HEIGHT = _getenv_int("BROWSER_WINDOW_HEIGHT", 2200)
YESSTYLE_MAX_PAGES = _getenv_int("YESSTYLE_MAX_PAGES", 1)

PROXY_USER =  _getenv_str("USERNAME")
PROXY_PASSWORD = _getenv_str("PASSWORD")
PROXY_HOST = _getenv_str("YESSTYLE_PROXY_HOST") or _getenv_str("PROXY_HOST", "isp.decodo.com")
PROXY_PORT = _getenv_str("YESSTYLE_PROXY_PORT") or _getenv_str("PROXY_PORT", "10000")

COUNTRIES: Sequence[Tuple[str, str]] = (
    ("Spain", "스페인"),
    ("Mexico", "멕시코"),
    ("Hong Kong", "홍콩"),
    ("Singapore", "싱가포르"),
    ("Taiwan", "대만"),
    ("Thailand", "태국"),
    ("Vietnam", "베트남"),
)

CATEGORY_CONFIGS: Sequence[Tuple[str, str]] = (
    ("Skincare", SKINCARE_URL),
    ("Numbuzin", NUMBUZIN_URL),
)

FILTER_BY_EXACT_BRAND = {"Numbuzin"}


def parse_countries_env(raw: str) -> Sequence[Tuple[str, str]]:
    countries: List[Tuple[str, str]] = []
    for part in raw.split(","):
        entry = part.strip()
        if not entry:
            continue
        if ":" in entry:
            english_country, label_country = entry.split(":", 1)
        else:
            english_country = entry
            label_country = entry
        english_country = english_country.strip()
        label_country = label_country.strip() or english_country
        if english_country:
            countries.append((english_country, label_country))
    return countries


COUNTRIES = parse_countries_env(_getenv_str("YESSTYLE_COUNTRIES")) or COUNTRIES


def clean_text(value: Optional[str]) -> str:
    if value is None:
        return ""
    return " ".join(value.replace("\xa0", " ").split())


def safe_text(locator) -> str:
    try:
        return clean_text(locator.text_content(timeout=5000))
    except Exception:
        return ""


def split_brand_and_product(full_title: str) -> Tuple[str, str]:
    parts = full_title.split("-", 1)
    brand = parts[0].strip() if parts else ""
    product = parts[1].strip() if len(parts) > 1 else full_title.strip()
    return brand, product


def build_datetime_text() -> str:
    now = datetime.now(SEOUL_TZ)
    return f"{now.strftime('%y')}년 {now.month}월 {now.day}일 {now.hour}시"


def build_paged_url(url: str, page_num: int) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["pn"] = str(page_num)
    return urlunparse(parsed._replace(query=urlencode(query)))


def wait_for_listing_items(page: Page, timeout_ms: int = 30000, require_items: bool = True) -> bool:
    if not require_items:
        page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
        return True

    locator = page.locator(ITEM_SELECTOR)
    deadline = datetime.now().timestamp() + (timeout_ms / 1000)
    while datetime.now().timestamp() < deadline:
        try:
            if locator.count() > 0:
                locator.first.wait_for(state="visible", timeout=2000)
                return True
        except PlaywrightTimeoutError:
            pass
        except PlaywrightError:
            pass
        page.wait_for_timeout(1000)

    raise PlaywrightTimeoutError(f"Timed out waiting for listing items: {page.url}")


def safe_goto(page: Page, url: str, attempts: int = 3, require_items: bool = True) -> bool:
    last_error: Optional[Exception] = None
    for attempt in range(1, attempts + 1):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            try:
                page.wait_for_load_state("networkidle", timeout=10000)
            except PlaywrightTimeoutError:
                logger.debug("networkidle timeout ignored for %s", url)
            return wait_for_listing_items(page, timeout_ms=30000, require_items=require_items)
        except (PlaywrightTimeoutError, PlaywrightError) as exc:
            last_error = exc
            logger.warning(
                "safe_goto attempt %s/%s failed for %s: %s",
                attempt,
                attempts,
                url,
                exc,
            )
            if attempt == attempts:
                raise
            page.wait_for_timeout(1500 * attempt)
    if last_error is not None:
        raise last_error
    return False


def set_shipping_destination(page: Page, url: str, english_country: str) -> None:
    safe_goto(page, url, require_items=False)

    country_button = page.locator(COUNTRY_BUTTON_SELECTOR)
    if country_button.count() > 0 and country_button.first.is_visible():
        country_button.first.click()
    else:
        page.get_by_role("button", name=re.compile("country setting", re.I)).click()

    dialog = page.get_by_role("dialog", name=re.compile("Preferences", re.I))
    dialog.wait_for(state="visible", timeout=15000)

    destination_input = dialog.locator("#shipping-destination-setting")
    destination_input.click()
    destination_input.fill("")
    destination_input.type(english_country, delay=60)

    option = page.get_by_role("option", name=english_country, exact=True)
    option.wait_for(state="visible", timeout=15000)
    option.click()

    selected_value = clean_text(destination_input.input_value())
    if selected_value != english_country:
        raise RuntimeError(
            f"Shipping destination selection failed: expected '{english_country}', got '{selected_value}'"
        )

    save_button = dialog.get_by_role("button", name=re.compile("^Save$", re.I))
    save_button.wait_for(state="visible", timeout=10000)
    if not save_button.is_enabled():
        logger.info(
            "Save button disabled after selecting shipping destination '%s'; assuming destination is already applied.",
            english_country,
        )
        dialog.get_by_role("button", name=re.compile("^Cancel$", re.I)).click()
        dialog.wait_for(state="hidden", timeout=10000)
        safe_goto(page, url, require_items=False)
        return

    save_button.click()
    dialog.wait_for(state="hidden", timeout=15000)
    safe_goto(page, url, require_items=False)


def extract_category_rows(
    page: Page,
    country_label: str,
    category_name: str,
    collected_at: str,
    page_num: int,
) -> List[Dict[str, Optional[str]]]:
    wait_for_listing_items(page, timeout_ms=20000, require_items=True)
    items = page.locator(ITEM_SELECTOR)
    item_count = items.count()
    if item_count == 0:
        return []

    limit = min(item_count, TARGET_COUNT)
    base_rank = (page_num - 1) * TARGET_COUNT
    rows: List[Dict[str, Optional[str]]] = []

    for index in range(limit):
        item = items.nth(index)
        full_title = safe_text(item.locator("div[class*='itemTitle']").first)
        brand, product_name = split_brand_and_product(full_title)

        if category_name in FILTER_BY_EXACT_BRAND and brand.casefold() != category_name.casefold():
            continue

        current_price = safe_text(item.locator("b[class*='itemPrice']").first) or None
        previous_price = safe_text(item.locator("span[class*='itemSellPrice']").first) or None

        rows.append(
            {
                COL_RANK: base_rank + index + 1,
                COL_COUNTRY: country_label,
                COL_BRAND: brand or None,
                COL_PRODUCT: product_name or None,
                COL_LIST_PRICE: previous_price,
                COL_PRICE: current_price,
                COL_SALES: None,
                COL_STORE: "Yesstyle",
                COL_COLLECTED_AT: collected_at,
                COL_CHANNEL: "YesStyle",
                COL_CATEGORY: category_name,
                COL_SPECIAL: None,
            }
        )

    return rows


def build_proxy_settings() -> Optional[dict]:
    if not PROXY_HOST or not PROXY_PORT:
        return None
    proxy_settings = {"server": f"http://{PROXY_HOST}:{PROXY_PORT}"}
    if PROXY_USER and PROXY_PASSWORD:
        proxy_settings["username"] = PROXY_USER
        proxy_settings["password"] = PROXY_PASSWORD
    return proxy_settings


def build_browser_launch_kwargs() -> Dict[str, object]:
    browser_launch_kwargs: Dict[str, object] = {
        "headless": HEADLESS,
        "args": ["--no-sandbox", "--disable-dev-shm-usage"],
    }
    proxy = build_proxy_settings()
    if proxy:
        browser_launch_kwargs["proxy"] = proxy
    return browser_launch_kwargs


def open_browser_page(playwright: Playwright) -> Tuple[Browser, BrowserContext, Page]:
    browser = playwright.chromium.launch(**build_browser_launch_kwargs())
    context = browser.new_context(
        viewport={"width": BROWSER_WINDOW_WIDTH, "height": BROWSER_WINDOW_HEIGHT},
        locale="en-US",
    )
    page = context.new_page()
    page.set_default_timeout(30000)
    return browser, context, page


def crawl_category_page_with_new_browser(
    playwright: Playwright,
    category_name: str,
    category_url: str,
    english_country: str,
    country_label: str,
    page_num: int,
    collected_at: str,
) -> List[Dict[str, Optional[str]]]:
    page_url = build_paged_url(category_url, page_num)
    logger.info("Opening fresh browser for %s/%s page %s: %s", category_name, country_label, page_num, page_url)
    browser, context, page = open_browser_page(playwright)
    try:
        set_shipping_destination(page, page_url, english_country)
        if not safe_goto(page, page_url, require_items=True):
            return []
        page.wait_for_timeout(1200)
        return extract_category_rows(page, country_label, category_name, collected_at, page_num)
    finally:
        context.close()
        browser.close()
        logger.info("Closed browser for %s/%s page %s", category_name, country_label, page_num)


def collect_rows() -> List[Dict[str, Optional[str]]]:
    all_rows: List[Dict[str, Optional[str]]] = []
    collected_at = build_datetime_text()
    proxy = build_proxy_settings()
    if proxy:
        logger.info("Launching Playwright Chromium with proxy server %s per page", proxy["server"])
    else:
        logger.info("Launching Playwright Chromium without proxy per page")

    with sync_playwright() as playwright:
        page_num = 1
        for english_country, country_label in COUNTRIES:
            for category_name, category_url in CATEGORY_CONFIGS:
                logger.info(
                    "Collecting %s rows for %s (%s) page %s",
                    category_name,
                    country_label,
                    english_country,
                    page_num,
                )
                try:
                    page_rows = crawl_category_page_with_new_browser(
                        playwright=playwright,
                        category_name=category_name,
                        category_url=category_url,
                        english_country=english_country,
                        country_label=country_label,
                        page_num=page_num,
                        collected_at=collected_at,
                    )
                except Exception as exc:
                    raise RuntimeError(
                        f"Failed while collecting {category_name}/{country_label} ({english_country}) page {page_num}: {exc}"
                    ) from exc

                if not page_rows:
                    logger.info(
                        "No rows for %s/%s page %s; skipping.",
                        category_name,
                        country_label,
                        page_num,
                    )
                    continue

                all_rows.extend(page_rows)
                logger.info(
                    "Collected %s rows for %s/%s page %s",
                    len(page_rows),
                    category_name,
                    country_label,
                    page_num,
                )

    return all_rows


def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_DATABASE,
        user=DB_USER,
        password=DB_PASSWORD,
    )


def ensure_table(connection) -> None:
    with connection.cursor() as cursor:
        cursor.execute(
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(SCHEMA_NAME))
        )
        cursor.execute(
            sql.SQL(
                """
                CREATE TABLE IF NOT EXISTS {} (
                    id BIGSERIAL PRIMARY KEY,
                    {} BIGINT,
                    {} TEXT,
                    {} TEXT,
                    {} TEXT,
                    {} TEXT,
                    {} TEXT,
                    {} TEXT,
                    {} TEXT,
                    {} TEXT,
                    {} TEXT,
                    {} TEXT,
                    {} TEXT,
                    created_at TIMESTAMP DEFAULT NOW()
                )
                """
            ).format(
                sql.Identifier(SCHEMA_NAME, TABLE_NAME),
                sql.Identifier(COL_RANK),
                sql.Identifier(COL_BRAND),
                sql.Identifier(COL_PRODUCT),
                sql.Identifier(COL_LIST_PRICE),
                sql.Identifier(COL_PRICE),
                sql.Identifier(COL_SALES),
                sql.Identifier(COL_STORE),
                sql.Identifier(COL_COLLECTED_AT),
                sql.Identifier(COL_CHANNEL),
                sql.Identifier(COL_CATEGORY),
                sql.Identifier(COL_SPECIAL),
                sql.Identifier(COL_COUNTRY),
            )
        )
        cursor.execute(
            sql.SQL(
                """
                ALTER TABLE {}
                ADD COLUMN IF NOT EXISTS {} TEXT
                """
            ).format(
                sql.Identifier(SCHEMA_NAME, TABLE_NAME),
                sql.Identifier(COL_COUNTRY),
            )
        )
        cursor.execute(
            sql.SQL(
                """
                ALTER TABLE {}
                ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT NOW()
                """
            ).format(sql.Identifier(SCHEMA_NAME, TABLE_NAME))
        )
    connection.commit()


def insert_rows(rows: Sequence[Dict[str, Optional[str]]]) -> None:
    if not rows:
        logger.warning("No rows to save.")
        return

    values = [
        (
            row[COL_RANK],
            row[COL_BRAND],
            row[COL_PRODUCT],
            row[COL_LIST_PRICE],
            row[COL_PRICE],
            row[COL_SALES],
            row[COL_STORE],
            row[COL_COLLECTED_AT],
            row[COL_CHANNEL],
            row[COL_CATEGORY],
            row[COL_SPECIAL],
            row[COL_COUNTRY],
        )
        for row in rows
    ]

    with get_connection() as connection:
        ensure_table(connection)
        with connection.cursor() as cursor:
            query = sql.SQL(
                """
                INSERT INTO {} ({}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {})
                VALUES %s
                """
            ).format(
                sql.Identifier(SCHEMA_NAME, TABLE_NAME),
                sql.Identifier(COL_RANK),
                sql.Identifier(COL_BRAND),
                sql.Identifier(COL_PRODUCT),
                sql.Identifier(COL_LIST_PRICE),
                sql.Identifier(COL_PRICE),
                sql.Identifier(COL_SALES),
                sql.Identifier(COL_STORE),
                sql.Identifier(COL_COLLECTED_AT),
                sql.Identifier(COL_CHANNEL),
                sql.Identifier(COL_CATEGORY),
                sql.Identifier(COL_SPECIAL),
                sql.Identifier(COL_COUNTRY),
            )
            execute_values(cursor, query.as_string(connection), values, page_size=200)
        connection.commit()

    logger.info("Inserted %s rows into %s.%s", len(rows), SCHEMA_NAME, TABLE_NAME)


def main() -> None:
    rows = collect_rows()
    insert_rows(rows)


if __name__ == "__main__":
    main()
