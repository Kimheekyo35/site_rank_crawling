import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse
from zoneinfo import ZoneInfo

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from playwright.sync_api import Browser, BrowserContext, Error as PlaywrightError
from playwright.sync_api import Page, Playwright, TimeoutError as PlaywrightTimeoutError, sync_playwright
from psycopg2 import Error

load_dotenv(override=True)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)


def _getenv_str(name: str, default: str = "") -> str:
    value = os.getenv(name)
    return value.strip() if value is not None else default


def _getenv_int(name: str) -> Optional[int]:
    raw = _getenv_str(name)
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        logger.warning("Invalid integer for %s=%r; treating as unset.", name, raw)
        return None


def _getenv_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


DB_HOST = _getenv_str("PG_HOST")
DB_PORT = _getenv_int("PG_PORT")
DB_DATABASE = _getenv_str("PG_DATABASE")
DB_USER = _getenv_str("PG_USER")
DB_PASSWORD = _getenv_str("PG_PASSWORD")

USE_DB = _getenv_bool("YESSTYLE_USE_DB", _getenv_bool("STYLEVANA_USE_DB", False))
HEADLESS = _getenv_bool("PLAYWRIGHT_HEADLESS", True)
BROWSER_WINDOW_WIDTH = _getenv_int("BROWSER_WINDOW_WIDTH") or 1440
BROWSER_WINDOW_HEIGHT = _getenv_int("BROWSER_WINDOW_HEIGHT") or 2200
YESSTYLE_MAX_PAGES = _getenv_int("YESSTYLE_MAX_PAGES")
YESSTYLE_PARALLEL_COUNTRIES = _getenv_int("YESSTYLE_PARALLEL_COUNTRIES") or 4
USE_PROXY = _getenv_bool("YESSTYLE_USE_PROXY", False)

PROXY_USER = os.getenv("USERNAME", "").strip()
PROXY_PASSWORD = os.getenv("PASSWORD", "").strip()
PROXY_HOST = os.getenv("PROXY_HOST", "").strip()
PROXY_PORT = os.getenv("PROXY_PORT", "").strip()

SEOUL_TZ = ZoneInfo("Asia/Seoul")

COL_RANK = "Rank"
COL_BRAND = "Brand"
COL_PRODUCT_NAME = "Product"
COL_PRICE = "Price"
COL_COUNTRY = "Country"
COL_DATETIME_TEXT = "Datetime"
COL_CHANNEL = "Channel"
COL_COLLECTED_AT = "CollectedAt"

COL_BRAND_KEY = "_brand_key"
COL_PRODUCT_KEY = "_product_key"

DEFAULT_BESTSELLER_URL = "https://www.yesstyle.com/en/beauty-sun-care/list.html/bcc.15600_bpt.46"
BESTSELLER_URL = _getenv_str("YESSTYLE_BESTSELLER_URL", DEFAULT_BESTSELLER_URL)

COUNTRY_BUTTON_SELECTOR = (
    "body > header > div.header-module-scss-module__lXl03a__mainHeaderToolWrapper.MuiBox-root.mui-0 "
    "> div > div.header-module-scss-module__lXl03a__toolsLeft.MuiBox-root.mui-0 > button:nth-child(1)"
)
ITEM_SELECTOR = "a[class*='itemContainer']"
NO_AVAILABLE_ITEMS_SELECTOR = (
    "body > main > div > div.productListingMain-module-scss-module__1cWHBG__productListWrapper "
    "> div.productListingMain-module-scss-module__1cWHBG__imageGridWrapper > div"
)

DEFAULT_COUNTRIES: Sequence[Tuple[str, str]] = (
    ("Spain", "스페인"),
    ("Mexico", "멕시코"),
    ("Hong Kong", "홍콩"),
    ("Singapore", "싱가포르"),
    ("Taiwan", "대만"),
    ("Thailand", "태국"),
    ("Vietnam", "베트남"),
    ("United States", "미국"),
)
CATEGORY_CONFIGS: Sequence[Tuple[str, str]] = (
    ("Bestsellers", BESTSELLER_URL),
)


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


COUNTRIES: Sequence[Tuple[str, str]] = parse_countries_env(_getenv_str("YESSTYLE_COUNTRIES")) or DEFAULT_COUNTRIES


def has_complete_db_config() -> bool:
    return all([DB_HOST, DB_PORT, DB_DATABASE, DB_USER, DB_PASSWORD])


def build_paged_url(url: str, page_num: int) -> str:
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query, keep_blank_values=True))
    query["pn"] = str(page_num)
    return urlunparse(parsed._replace(query=urlencode(query)))


def clean_text(value: Optional[str]) -> str:
    if value is None:
        return ""
    return " ".join(value.replace("\xa0", " ").split())


def safe_text(locator) -> str:
    try:
        return clean_text(locator.text_content(timeout=5000))
    except Exception:
        return ""


def has_no_available_items(page: Page) -> bool:
    try:
        locator = page.locator(NO_AVAILABLE_ITEMS_SELECTOR)
        count = locator.count()
        for index in range(count):
            element = locator.nth(index)
            if not element.is_visible():
                continue
            if clean_text(element.inner_text(timeout=2000)) == "No available items":
                return True
    except Exception:
        return False
    return False


def wait_for_listing_items(page: Page, timeout_ms: int = 30000, require_items: bool = True) -> bool:
    if not require_items:
        page.wait_for_load_state("domcontentloaded", timeout=timeout_ms)
        return True

    deadline = datetime.now().timestamp() + (timeout_ms / 1000)
    locator = page.locator(ITEM_SELECTOR)
    while datetime.now().timestamp() < deadline:
        if has_no_available_items(page):
            logger.info("Detected 'No available items' at %s", page.url)
            return False
        try:
            count = locator.count()
            if count > 0:
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
        cancel_button = dialog.get_by_role("button", name=re.compile("^Cancel$", re.I))
        cancel_button.click()
        dialog.wait_for(state="hidden", timeout=10000)
        safe_goto(page, url, require_items=False)
        return

    save_button.click()
    dialog.wait_for(state="hidden", timeout=15000)
    safe_goto(page, url, require_items=False)


def _extract_primary_price(text: str) -> str:
    if not text:
        return "N/A"
    match = re.search(r"([$€£¥₩]|SG\$|HK\$|NT\$|MX\$|US\$|C\$|A\$)?\s?\d[\d,]*(?:\.\d+)?", text)
    if match:
        return match.group(0).strip()
    return text.strip()


def _normalize_product_name(name: str) -> str:
    if not name:
        return ""
    text = re.sub(r"^\(\d+(?:ML|EA|G|PATCHES|PCS|PADS)\)\s*", "", name.strip(), flags=re.I)
    return re.sub(r"\s+", " ", text)


def _normalize_match_key(value: Optional[str]) -> str:
    if value is None:
        return ""
    return re.sub(r"\s+", "", str(value))


def _add_match_keys_inplace(df: pd.DataFrame) -> pd.DataFrame:
    df[COL_BRAND_KEY] = df[COL_BRAND].apply(_normalize_match_key) if COL_BRAND in df.columns else ""
    df[COL_PRODUCT_KEY] = df[COL_PRODUCT_NAME].apply(_normalize_match_key) if COL_PRODUCT_NAME in df.columns else ""
    return df


def parse_list_grid_products(page: Page, start_rank: int = 1) -> Tuple[List[str], List[str], List[int], List[str]]:
    items = page.locator(ITEM_SELECTOR)
    count = items.count()

    names: List[str] = []
    prices: List[str] = []
    ranks: List[int] = []
    brands: List[str] = []
    rank = start_rank

    for index in range(count):
        item = items.nth(index)
        raw_title = safe_text(item.locator("div[class*='itemTitle']").first) or "N/A"
        normalized_title = _normalize_product_name(raw_title)

        brand = "N/A"
        product = normalized_title
        if "-" in normalized_title:
            brand_part, product_part = normalized_title.split("-", 1)
            brand = brand_part.strip() or "N/A"
            product = _normalize_product_name(product_part)

        raw_price = ""
        for selector in ("b[class*='itemPrice']", "[class*='itemPrice']"):
            raw_price = safe_text(item.locator(selector).first)
            if raw_price:
                break
        if not raw_price:
            raw_price = "N/A"

        names.append(product or "N/A")
        prices.append(_extract_primary_price(raw_price))
        ranks.append(rank)
        brands.append(brand)
        rank += 1

    return names, prices, ranks, brands


def crawl_yesstyle_page(page: Page, url: str, start_rank: int) -> Tuple[List[str], List[str], List[int], List[str]]:
    if not safe_goto(page, url, require_items=True):
        return [], [], [], []
    page.wait_for_timeout(1200)
    return parse_list_grid_products(page, start_rank=start_rank)


def crawl_page_with_new_browser(
    playwright: Playwright,
    base_url: str,
    english_country: str,
    page_num: int,
    start_rank: int,
) -> Tuple[List[str], List[str], List[int], List[str]]:
    page_url = build_paged_url(base_url, page_num)
    logger.info("Opening fresh browser for page %s: %s", page_num, page_url)

    browser, context, page = open_browser_page(playwright)
    try:
        set_shipping_destination(page, page_url, english_country)
        return crawl_yesstyle_page(page, page_url, start_rank=start_rank)
    finally:
        context.close()
        browser.close()
        logger.info("Closed browser for page %s", page_num)


def yesstyle_scroll_crawling(
    playwright: Playwright,
    url: str,
    english_country: str,
    max_pages: Optional[int] = None,
) -> Tuple[List[str], List[str], List[int], List[str]]:
    gathered_names: List[str] = []
    gathered_prices: List[str] = []
    gathered_ranks: List[int] = []
    gathered_brands: List[str] = []
    page_num = 1

    while True:
        if max_pages is not None and page_num > max_pages:
            break

        try:
            names, prices, ranks, brands = crawl_page_with_new_browser(
                playwright,
                url,
                english_country,
                page_num,
                start_rank=len(gathered_names) + 1,
            )
        except PlaywrightTimeoutError as exc:
            logger.info("Stopping pagination at page %s because listing items timed out: %s", page_num, exc)
            break

        if not names:
            break

        gathered_names.extend(names)
        gathered_prices.extend(prices)
        gathered_ranks.extend(ranks)
        gathered_brands.extend(brands)
        page_num += 1

    return gathered_names, gathered_prices, gathered_ranks, gathered_brands


def build_proxy_settings() -> Optional[dict]:
    if not USE_PROXY:
        return None
    if not PROXY_HOST or not PROXY_PORT:
        logger.warning("YESSTYLE_USE_PROXY=true but PROXY_HOST/PROXY_PORT is missing; continuing without proxy.")
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


def collect_country_category_frames(
    english_country: str,
    label_country: str,
    iso_timestamp: str,
) -> Dict[str, pd.DataFrame]:
    frames_by_category: Dict[str, pd.DataFrame] = {}

    with sync_playwright() as playwright:
        for category_name, category_url in CATEGORY_CONFIGS:
            logger.info(
                "Start crawling %s for %s (%s)...",
                category_name,
                label_country,
                english_country,
            )
            names, prices, ranks, brands = yesstyle_scroll_crawling(
                playwright,
                category_url,
                english_country,
                max_pages=YESSTYLE_MAX_PAGES,
            )

            if not names:
                logger.warning("No products found for %s in %s; skipping.", category_name, label_country)
                continue

            df = pd.DataFrame(
                {
                    COL_RANK: ranks,
                    COL_BRAND: brands,
                    COL_PRODUCT_NAME: names,
                    COL_PRICE: prices,
                    COL_COUNTRY: [label_country] * len(names),
                    COL_DATETIME_TEXT: iso_timestamp,
                    COL_CHANNEL: ["yesstyle"] * len(names),
                    COL_COLLECTED_AT: [iso_timestamp] * len(names),
                }
            )
            _add_match_keys_inplace(df)
            frames_by_category[category_name] = df.dropna(subset=[COL_RANK, COL_PRODUCT_NAME]).copy()

    return frames_by_category



def collect_category_frames() -> Dict[str, pd.DataFrame]:
    run_time = datetime.now(SEOUL_TZ)
    iso_timestamp = run_time.strftime("%Y-%m-%d %H:%M:%S")
    category_frames_by_name: Dict[str, List[pd.DataFrame]] = {
        category_name: [] for category_name, _ in CATEGORY_CONFIGS
    }

    proxy = build_proxy_settings()
    if proxy:
        logger.info("Launching Playwright Chromium with proxy server %s", proxy["server"])
    else:
        logger.info("Launching Playwright Chromium without proxy")

    max_workers = max(1, min(YESSTYLE_PARALLEL_COUNTRIES, len(COUNTRIES)))
    logger.info("Processing %s countries with up to %s concurrent workers", len(COUNTRIES), max_workers)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(
                collect_country_category_frames,
                english_country,
                label_country,
                iso_timestamp,
            ): (english_country, label_country)
            for english_country, label_country in COUNTRIES
        }

        for future in as_completed(futures):
            english_country, label_country = futures[future]
            try:
                frames_by_category = future.result()
            except Exception:
                logger.exception(
                    "Country crawl failed for %s (%s).",
                    label_country,
                    english_country,
                )
                continue

            for category_name, frame in frames_by_category.items():
                if frame.empty:
                    continue
                category_frames_by_name[category_name].append(frame)

    category_frames: Dict[str, pd.DataFrame] = {}
    for category_name, _ in CATEGORY_CONFIGS:
        frames = category_frames_by_name.get(category_name, [])
        if frames:
            category_frames[category_name] = pd.concat(frames, ignore_index=True)
        else:
            category_frames[category_name] = pd.DataFrame(
                columns=[
                    COL_RANK,
                    COL_BRAND,
                    COL_PRODUCT_NAME,
                    COL_PRICE,
                    COL_COUNTRY,
                    COL_DATETIME_TEXT,
                    COL_CHANNEL,
                    COL_COLLECTED_AT,
                ]
            )

    for df in category_frames.values():
        if df.empty:
            continue
        df[COL_RANK] = pd.to_numeric(df[COL_RANK], errors="coerce")
        df.dropna(subset=[COL_RANK, COL_PRODUCT_NAME], inplace=True)
        df[COL_RANK] = df[COL_RANK].astype(int)
        for col in (COL_BRAND, COL_PRICE, COL_CHANNEL, COL_COUNTRY):
            if col in df.columns:
                df[col] = df[col].fillna("")
        df.sort_values(by=[COL_COUNTRY, COL_RANK], inplace=True)
        df.reset_index(drop=True, inplace=True)
        _add_match_keys_inplace(df)

    return category_frames


def _ensure_empty_pgpass() -> None:
    pgpass_path = Path(__file__).resolve().parent / ".pgpass_empty"
    try:
        if not pgpass_path.exists():
            pgpass_path.write_text("", encoding="utf-8")
        os.environ["PGPASSFILE"] = str(pgpass_path)
    except Exception as exc:
        logger.warning("PGPASSFILE setting failed: %s", exc)


def ensure_yesstyle_table_exists(connection) -> None:
    cursor = connection.cursor()
    cursor.execute("CREATE SCHEMA IF NOT EXISTS suncream_crawling;")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS suncream_crawling.yesstyle (
            id BIGSERIAL PRIMARY KEY,
            "Rank" INTEGER,
            "Brand" VARCHAR(255),
            "Product" TEXT,
            "Price" VARCHAR(50),
            "DateTime" TIMESTAMP,
            "Channel" VARCHAR(50),
            "Country" VARCHAR(100),
            "Old_price" VARCHAR(50),
            created_at TIMESTAMP DEFAULT NOW()
        );
        """
    )
    cursor.execute(
        """
        ALTER TABLE suncream_crawling.yesstyle
        ADD COLUMN IF NOT EXISTS "Country" VARCHAR(100)
        """
    )
    connection.commit()
    cursor.close()


def insert_into_postgresql(rows: List[Tuple]) -> None:
    if not rows:
        return
    if not USE_DB:
        logger.info("DB save disabled (YESSTYLE_USE_DB=false); skipping insert.")
        return
    if not has_complete_db_config():
        logger.warning("DB config is incomplete; skipping insert into suncream_crawling.yesstyle.")
        return

    connection = None
    try:
        _ensure_empty_pgpass()
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_DATABASE,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        ensure_yesstyle_table_exists(connection)
        cursor = connection.cursor()
        query = """
            INSERT INTO suncream_crawling.yesstyle
                ("Rank", "Brand", "Product", "Price", "DateTime", "Channel", "Country")
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(query, rows)
        connection.commit()
        cursor.close()
        logger.info("Inserted %s rows into suncream_crawling.yesstyle.", len(rows))
    except Error:
        if connection:
            connection.rollback()
        logger.exception("PostgreSQL insert error")
    finally:
        if connection:
            connection.close()


def tage_table(connection) -> None:
    cursor = connection.cursor()
    cursor.execute("CREATE SCHEMA IF NOT EXISTS suncream_crawling;")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS suncream_crawling.yesstyle_tage (
            id BIGSERIAL PRIMARY KEY,
            "Rank" INTEGER,
            "Brand" VARCHAR(255),
            "Product" TEXT,
            "Price" VARCHAR(50),
            "DateTime" TIMESTAMP,
            "Channel" VARCHAR(50),
            "Country" VARCHAR(100),
            created_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'Asia/Seoul')
        );
        """
    )
    cursor.execute(
        """
        ALTER TABLE suncream_crawling.yesstyle_tage
        ADD COLUMN IF NOT EXISTS "Country" VARCHAR(100)
        """
    )
    cursor.execute(
        """
        ALTER TABLE suncream_crawling.yesstyle_tage
        DROP COLUMN IF EXISTS "Old_price";
        """
    )
    connection.commit()
    cursor.close()


def insert_into_postgresql_tage(rows: List[Tuple]) -> None:
    if not rows:
        return
    if not USE_DB:
        logger.info("DB save disabled (YESSTYLE_USE_DB=false); skipping Tage insert.")
        return
    if not has_complete_db_config():
        logger.warning("DB config is incomplete; skipping insert into suncream_crawling.yesstyle_tage.")
        return

    tage_rows = [row for row in rows if len(row) > 1 and str(row[1]).strip().lower() == "tage"]
    if not tage_rows:
        logger.info("No rows with Brand='Tage'; skipping yesstyle_tage insert.")
        return

    connection = None
    try:
        _ensure_empty_pgpass()
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_DATABASE,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        tage_table(connection)
        cursor = connection.cursor()
        query = """
            INSERT INTO suncream_crawling.yesstyle_tage
                ("Rank", "Brand", "Product", "Price", "DateTime", "Channel", "Country")
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(query, tage_rows)
        connection.commit()
        cursor.close()
        logger.info("Inserted %s rows into suncream_crawling.yesstyle_tage.", len(tage_rows))
    except Error:
        if connection:
            connection.rollback()
        logger.exception("PostgreSQL insert error")
    finally:
        if connection:
            connection.close()


def main() -> None:
    category_frames = collect_category_frames()
    bestseller_df = category_frames.get("Bestsellers", pd.DataFrame())

    db_rows: List[Tuple] = []
    all_bestseller_rows: List[Tuple] = []

    if not bestseller_df.empty:
        for _, country_df in bestseller_df.groupby(COL_COUNTRY, sort=False, dropna=False):
            top_country_df = country_df.sort_values(by=COL_RANK).head(100)
            for _, row in top_country_df.iterrows():
                country_value = row.get(COL_COUNTRY)
                if pd.isna(country_value) or str(country_value).strip() == "":
                    country_value = None
                db_rows.append(
                    (
                        int(row[COL_RANK]),
                        row[COL_BRAND],
                        row[COL_PRODUCT_NAME],
                        row[COL_PRICE],
                        row[COL_DATETIME_TEXT],
                        row[COL_CHANNEL],
                        country_value,
                    )
                )

        for _, row in bestseller_df.iterrows():
            country_value = row.get(COL_COUNTRY)
            if pd.isna(country_value) or str(country_value).strip() == "":
                country_value = None
            all_bestseller_rows.append(
                (
                    int(row[COL_RANK]),
                    row[COL_BRAND],
                    row[COL_PRODUCT_NAME],
                    row[COL_PRICE],
                    row[COL_DATETIME_TEXT],
                    row[COL_CHANNEL],
                    country_value,
                )
            )

    insert_into_postgresql(db_rows)
    insert_into_postgresql_tage(all_bestseller_rows)
    logger.info("YesStyle Tage crawling completed.")


if __name__ == "__main__":
    main()
