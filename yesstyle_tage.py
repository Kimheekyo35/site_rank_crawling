import os
import time
import re
import random
import tempfile
import shutil
from pathlib import Path
from typing import Optional, Iterable, Tuple, List

import pandas as pd
import psycopg2
from psycopg2 import Error
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.remote.webdriver import WebDriver
from seleniumbase import Driver
from datetime import datetime

# --------------------------------------------------------------------------- #
# 환경 설정
# --------------------------------------------------------------------------- #
load_dotenv(override=True)

PROXY_USER = os.getenv("USERNAME", "").strip()
PROXY_PASSWORD = os.getenv("PASSWORD", "").strip()
PROXY_HOST = os.getenv("PROXY_HOST", "isp.decodo.com").strip()
PROXY_PORT = os.getenv("PROXY_PORT", "10000").strip()

DB_HOST = os.getenv("PG_HOST")
DB_PORT = int(os.getenv("PG_PORT"))
# target DB 고정: wemarketing_db (필요 시 .env의 DB_DATABASE로 오버라이드)
DB_DATABASE = os.getenv("PG_DATABASE")
DB_USER = os.getenv("PG_USER")
DB_PASSWORD = os.getenv("PG_PASSWORD")
USE_DB = os.getenv("STYLEVANA_USE_DB", "true").lower() in ("1", "true", "yes")

SEOUL_TZ = ZoneInfo("Asia/Seoul")

# 컬럼 이름 상수
COL_RANK = "Rank"
COL_BRAND = "Brand"
COL_PRODUCT_NAME = "Product"
COL_PRICE = "Price"
COL_DATETIME_TEXT = "Datetime"
COL_CHANNEL = "Channel"

COL_COLLECTED_AT = "수집일시"

COL_PREVIOUS_RANK = "전일 순위"
COL_RANK_DELTA = "전일 변동"
COL_STATUS = "전일대비 증감"

COL_PREVIOUS_RANK_WEEK = "전주 순위"
COL_WEEK_DELTA = "전주 변동"
COL_WEEK_STATUS = "전주대비 증감"

# 크롤링 대상 URL
# 기본값은 YesStyle 베스트셀러 페이지이고,
# 환경변수(또는 .env)의 YESSTYLE_BESTSELLER_URL 값으로 덮어쓸 수 있습니다.
DEFAULT_BESTSELLER_URL = "https://www.yesstyle.com/en/beauty-sun-care/list.html/bcc.15600_bpt.46"
BESTSELLER_URL = os.getenv("YESSTYLE_BESTSELLER_URL", DEFAULT_BESTSELLER_URL)
# SKINCARE_URL = "https://www.yesstyle.com/en/beauty-skin-care/list.html/bcc.15544_bpt.46"
# NUMBUZIN_URL = "https://www.yesstyle.com/en/numbuzin/list.html/bpt.299_bid.326359"

COL_BRAND_KEY = "_brand_key"    
COL_PRODUCT_KEY = "_product_key"


def build_proxy_url() -> str:
    if not PROXY_HOST or not PROXY_PORT:
        return ""
    if PROXY_USER and PROXY_PASSWORD:
        return f"{PROXY_USER}:{PROXY_PASSWORD}@{PROXY_HOST}:{PROXY_PORT}"
    return f"{PROXY_HOST}:{PROXY_PORT}"

# --------------------------------------------------------------------------- #
# 유틸 함수
# --------------------------------------------------------------------------- #
def ensure_data_directory() -> Path:
    base = Path(__file__).resolve().parent
    data_dir = base / "data"
    data_dir.mkdir(exist_ok=True)
    return data_dir


def fill_missing(items: List[Optional[str]], target: int) -> List[Optional[str]]:
    if len(items) < target:
        items.extend([None] * (target - len(items)))
    return items

def _extract_primary_price(text: str) -> str:
    if not text:
        return "N/A"
    match = re.search(r"([₩￦$€£¥])\s?\d[\d,]*(?:\.\d+)?", text)
    if match:
        return match.group(0).strip()
    num = re.search(r"\d[\d,]*(?:\.\d+)?", text)
    if num:
        prefix = text[:num.start()].strip()
        symbol = ""
        if prefix and prefix[-1] in "₩￦$€£¥":
            symbol = prefix[-1]
        return f"{symbol} {num.group(0)}".strip()
    return text.strip()


VOLUME_PREFIX_PATTERN = re.compile(r"^\(\d+(?:ML|EA|G|PATCHES|PCS|PADS)\)\s*", re.IGNORECASE)


def _normalize_product_name(name: str) -> str:
    if name is None:
        return ""
    text = str(name).strip()
    text = VOLUME_PREFIX_PATTERN.sub("", text)
    text = re.sub(r"\s+", " ", text)
    return text


def _normalize_match_key(value: Optional[str]) -> str:
    if value is None:
        return ""
    text = str(value)
    return re.sub(r"\s+", "", text)


def _add_match_keys_inplace(df: pd.DataFrame) -> pd.DataFrame:
    if df is None:
        return df
    if COL_BRAND in df.columns:
        df[COL_BRAND_KEY] = df[COL_BRAND].apply(_normalize_match_key)
    else:
        df[COL_BRAND_KEY] = ""
    if COL_PRODUCT_NAME in df.columns:
        df[COL_PRODUCT_KEY] = df[COL_PRODUCT_NAME].apply(_normalize_match_key)
    else:
        df[COL_PRODUCT_KEY] = ""
    return df


def _deduplicate_by_match_keys(df: Optional[pd.DataFrame], limit: Optional[int] = None) -> Optional[pd.DataFrame]:
    if df is None:
        return None
    work = df.copy()
    _add_match_keys_inplace(work)
    work = work.drop_duplicates(subset=[COL_BRAND_KEY, COL_PRODUCT_KEY])
    if limit is not None:
        work = work.head(limit)
    work.reset_index(drop=True, inplace=True)
    return work


def parse_list_grid_products(products: Iterable, start_rank: int = 1):
    names, prices, ranks, brands = [], [], [], []
    rank = start_rank
    for item in products:
        try:
            raw_name = item.find_element(By.CSS_SELECTOR, "[class*='itemTitle']").text
        except Exception:
            raw_name = "N/A"

        normalized_name = _normalize_product_name(raw_name)
        brand = "N/A"
        product = normalized_name
        if "-" in normalized_name:
            brand_part, product_part = normalized_name.split("-", 1)
            brand_candidate = brand_part.strip()
            product = _normalize_product_name(product_part)
            brand = brand_candidate if brand_candidate else "N/A"

        try:
            raw_price = item.find_element(By.CSS_SELECTOR, "[class*='itemPrice']").text
        except Exception:
            raw_price = "N/A"

        names.append(product)
        prices.append(_extract_primary_price(raw_price))
        ranks.append(rank)
        brands.append(brand)
        rank += 1
    return names, prices, ranks, brands


def yesstyle_scroll_crawling(
    driver: WebDriver,
    url: str,       
    product_selector: str = "a[class*='itemContainer']",
) -> Tuple[
    List[Optional[str]],
    List[Optional[str]],
    List[Optional[int]],
    List[Optional[str]],
]:
    driver.get(url)
    time.sleep(random.uniform(2.5, 3.5))

    gathered_names: List[Optional[str]] = []
    gathered_prices: List[Optional[str]] = []
    gathered_ranks: List[Optional[int]] = []
    gathered_brands: List[Optional[str]] = []

    page_index = 0
    while True:
        page_index += 1
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, product_selector))
            )
        except Exception:
            pass

        products = driver.find_elements(By.CSS_SELECTOR, product_selector)
        if not products:
            print(f"[WARN] No products found with selector '{product_selector}' at {url}.")
            break

        names, prices, ranks, brands = parse_list_grid_products(
            products, start_rank=len(gathered_names) + 1
        )
        gathered_names.extend(names)
        gathered_prices.extend(prices)
        gathered_ranks.extend(ranks)
        gathered_brands.extend(brands)

        first_old = products[0]
        if not _find_and_click_next(driver):
            break
        try:
            WebDriverWait(driver, 10).until(EC.staleness_of(first_old))
        except Exception:
            pass
        time.sleep(random.uniform(1.0, 2.0))

    return gathered_names, gathered_prices, gathered_ranks, gathered_brands


def _find_and_click_next(driver: WebDriver) -> bool:
    selectors = [
        "a[class*='simpleDirectionButton'][class*='nextPage']",
        "a[class*='nextPage']",
        "button[class*='productListingMain_blackButton__']",
    ]
    for selector in selectors:
        try:
            elements = WebDriverWait(driver, 5).until(
                lambda d: d.find_elements(By.CSS_SELECTOR, selector)
            )
            element = elements[-1]
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
            WebDriverWait(driver, 5).until(lambda d: element.is_displayed() and element.is_enabled())
            driver.execute_script("arguments[0].click();", element)
            time.sleep(0.2)
            return True
        except Exception:
            continue
    return False


# --------------------------------------------------------------------------- #
# DB 관련 함수
# --------------------------------------------------------------------------- #
def _ensure_empty_pgpass():
    """
    Windows에서 홈 디렉터리 경로/파일 인코딩 문제로 libpq가 .pgpass 읽기 중
    UnicodeDecodeError가 날 수 있어, ASCII 경로의 빈 pgpass 파일을 강제로 지정합니다.
    """
    pgpass_path = Path(__file__).resolve().parent / ".pgpass_empty"
    try:
        if not pgpass_path.exists():
            pgpass_path.write_text("", encoding="utf-8")
        os.environ["PGPASSFILE"] = str(pgpass_path)
    except Exception as exc:
        # 실패해도 치명적이지 않으므로 로그만 남김
        print(f"[WARN] PGPASSFILE 설정 실패: {exc}")


def ensure_yesstyle_table_exists(connection) -> None:
    """
    suncream_crawling 스키마와 yesstyle 테이블이 없으면 생성합니다.
    """
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
            "Old_price" VARCHAR(50),
            created_at TIMESTAMP DEFAULT NOW()
        );
        """
    )
    connection.commit()
    cursor.close()


def insert_into_postgresql(rows: List[Tuple]):
    if not rows:
        return
    if not USE_DB:
        print("DB 저장 비활성화(YESSTYLE_USE_DB=false); 저장을 건너뜁니다.")
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
                ("Rank", "Brand", "Product", "Price", "DateTime", "Channel")
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(query, rows)
        connection.commit()
        cursor.close()
        print(f"Inserted {len(rows)} rows into suncream_crawling.yesstyle.")
    except Error as exc:
        if connection:
            connection.rollback()
        print(f"PostgreSQL insert error: {exc}")
    finally:
        if connection:
            connection.close()

def delete_db_col():
    if not USE_DB:  
        print("DB 저장 비활성화(YESSTYLE_USE_DB=false); 저장을 건너뜁니다.")
        return
    connection = None
    try:
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_DATABASE,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        ensure_yesstyle_table_exists(connection)
        
        with connection.cursor() as cursor:
            cursor.execute(
                """
                ALTER TABLE suncream_crawling.yesstyle
                DROP COLUMN IF EXISTS "Old_price"
                """
            )
        connection.commit()
        cursor.close()
    finally:
        if connection:
            connection.close()

def tage_table(connection=None):
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
            created_at TIMESTAMP DEFAULT (NOW() AT TIME ZONE 'Asia/Seoul')
        );

        """
    )
    connection.commit()
    cursor.close()


def insert_into_postgresql_tage(rows: List[Tuple]):
    if not rows:
        return
    if not USE_DB:
        print("DB 저장 비활성화(YESSTYLE_USE_DB=false); 저장을 건너뜁니다.")
        return
    tage_rows = [row for row in rows if len(row) > 1 and str(row[1]).strip().lower() == "tage"]
    if not tage_rows:
        print("Brand가 Tage인 데이터가 없어 yesstyle_tage 저장을 건너뜁니다. ")
    
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
                ("Rank", "Brand", "Product", "Price", "DateTime", "Channel")
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(query, tage_rows)
        connection.commit()
        cursor.close()

        print(f"Inserted {len(tage_rows)} rows into suncream_crawling.yesstyle_tage .")
    
    except Error as exc:
        if connection:
            connection.rollback()
        print(f"PostgreSQL insert error: {exc}")
    
    finally:
        if connection:
            connection.close()
# --------------------------------------------------------------------------- #
# 메인 로직
# --------------------------------------------------------------------------- #
def main():
    temp_profile = tempfile.mkdtemp(prefix="yesstyle_profile_")
    # proxy_url = build_proxy_url()
    driver = Driver(
        browser="chrome",
        uc=True,
        # proxy=proxy_url or None,
        user_data_dir=temp_profile,
        chromium_arg="--start-maximized,--no-sandbox,--disable-dev-shm-usage,"
        "--disable-notifications,--disable-background-networking",
    )

    run_time = datetime.now(SEOUL_TZ)
    run_time_naive = run_time.astimezone(SEOUL_TZ).replace(tzinfo=None)
    # date_str = f"{run_time.strftime('%y')}년 {run_time.month}월 {run_time.day}일 {run_time.hour}시"
    iso_timestamp = run_time.strftime("%Y-%m-%d %H:%M:%S")

    category_configs = [
        {"name": "Bestsellers", "url": BESTSELLER_URL},
    ]

    category_frames: dict[str, pd.DataFrame] = {}
    try:
        for config in category_configs:
            print(f"Start crawling {config['name']}...")
            names, prices, ranks, brands = yesstyle_scroll_crawling(
                driver,
                config["url"],
            )
            df = pd.DataFrame(
                {
                    COL_RANK: ranks,
                    COL_BRAND: brands,
                    COL_PRODUCT_NAME: names,
                    COL_PRICE: prices,
                    COL_DATETIME_TEXT:iso_timestamp ,
                    COL_CHANNEL: ["yesstyle"] * len(names),
                }
            )
            df[COL_COLLECTED_AT] = iso_timestamp
            _add_match_keys_inplace(df)
            category_frames[config["name"]] = df.dropna(subset=[COL_RANK, COL_PRODUCT_NAME]).copy()
    finally:
        driver.quit()
        shutil.rmtree(temp_profile, ignore_errors=True)

    for df in category_frames.values():
        df[COL_RANK] = pd.to_numeric(df[COL_RANK], errors="coerce")
        df.dropna(subset=[COL_RANK, COL_PRODUCT_NAME], inplace=True)
        df[COL_RANK] = df[COL_RANK].astype(int)
        for col in (COL_BRAND, COL_PRICE, COL_CHANNEL):
            if col in df.columns:
                df[col] = df[col].fillna("")
        df.sort_values(by=COL_RANK, inplace=True)
        df.reset_index(drop=True, inplace=True)
        _add_match_keys_inplace(df)

    combined_df_all = pd.concat(category_frames.values(), ignore_index=True)

    data_dir = ensure_data_directory()
    
    db_rows = []
    for _, row in combined_df_all.iterrows():
        price = row[COL_PRICE]
        db_rows.append(
            (
                int(row[COL_RANK]),
                row[COL_BRAND],
                row[COL_PRODUCT_NAME],
                price,
                row[COL_DATETIME_TEXT],
                row[COL_CHANNEL],
            )
        )
    insert_into_postgresql(db_rows[:100])
    insert_into_postgresql_tage(db_rows[100:])
    print("YesStyle 크롤링 완료.")


if __name__ == "__main__":
    main()
