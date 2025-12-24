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
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from dotenv import load_dotenv
from openpyxl.styles import Alignment, Font
from openpyxl.utils import get_column_letter

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from seleniumbase import SB  # type: ignore
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException
"""
StyleVana 크롤링 파이프라인 개요
1) SeleniumBase(undetected-chromedriver)로 상품 리스트 페이지를 열고 팝업을 닫은 뒤, 상품 카드들을 수집한다.
2) 카드별 이름/브랜드/현재가/정가를 파싱하고 DataFrame으로 정리한다.
3) (옵션) DB에 저장하고, 엑셀 리포트를 생성한 후 이메일로 전송한다.
"""
# --------------------------------------------------------------------------- #
# 환경 설정
#  - .env에서 DB/이메일 등을 읽고, 컬럼 상수와 기본 URL을 정의합니다.
# --------------------------------------------------------------------------- #
load_dotenv(override=True)

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
COL_OLD_PRICE = "Old_price"
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

DEFAULT_BESTSELLER_URL = "https://www.stylevana.com/en_US/skincare/suncare.html"
BESTSELLER_URL = os.getenv("STYLEVANA_URL",DEFAULT_BESTSELLER_URL)

CRAWL_LIMIT = 100

REPORT_LIMIT = 50
COL_BRAND_KEY = "_brand_key"
COL_PRODUCT_KEY = "_product_key"

# --------------------------------------------------------------------------- #
# 유틸 함수
#  - 디렉터리 생성, 가격/이름 정제, 매칭 키 생성 등 데이터 전처리용
# --------------------------------------------------------------------------- #
def ensure_data_directory() -> Path:
    """스크립트 실행 결과물을 모아둘 data/ 디렉터리를 생성(없으면)한 뒤 경로를 반환한다."""
    base = Path(__file__).resolve().parent
    data_dir = base / "data"
    data_dir.mkdir(exist_ok=True)
    return data_dir

VOLUME_PREFIX_PATTERN = re.compile(r"^\(\d+(?:ML|EA|G|PATCHES|PCS|PADS)\)\s*", re.IGNORECASE)


def _normalize_product_name(name: str) -> str:
    """Remove 용량 prefix와 중복 공백을 정리해 비교 가능한 상품명을 만든다."""
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
    """브랜드/제품명을 공백 제거한 키로 만들어 중복 제거/매칭에 사용합니다."""
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
_PRICE_RE = re.compile(r"\$?\s*([\d,.]+)")


def _extract_primary_price(text: str) -> str:
    if not text:
        return "N/A"
    m = _PRICE_RE.search(text)
    return m.group(1) if m else "N/A"


def _normalize_product_name(name: str) -> str:
    if name is None:
        return ""
    return re.sub(r"\s+", " ", str(name)).strip()


def fill_missing(lst: List, target_count: int, fill_value="") -> None:
    while len(lst) < target_count:
        lst.append(fill_value)


def _safe_find_attr(el, css: str, attr: str) -> Optional[str]:
    try:
        node = el.find_element(By.CSS_SELECTOR, css)
        val = node.get_attribute(attr)
        return val.strip() if val else None
    except NoSuchElementException:
        return None


def _safe_find_text(el, css: str) -> Optional[str]:
    try:
        node = el.find_element(By.CSS_SELECTOR, css)
        txt = node.text  # ✅ text() 아님
        return txt.strip() if txt else None
    except NoSuchElementException:
        return None
    
def _safe_find(el, by, selector):
    try:
        return el.find_element(by, selector)
    except (NoSuchElementException, StaleElementReferenceException):
        return None
    
def _extract_price_amount(item, price_type: str) -> str:
    """
    price_type: 'finalPrice' or 'rrpPrice'
    <span data-price-type="finalPrice" data-price-amount="32.4"> ... </span>
    """
    el = _safe_find(item, By.CSS_SELECTOR, f"[data-price-type='{price_type}']")
    if not el:
        return ""
    # 우선 data-price-amount 속성에서 읽기
    amt = el.get_attribute("data-price-amount") or ""
    # 없으면 내부에 data-price-amount가 있는 하위 span 탐색
    if not amt:
        inner = _safe_find(el, By.CSS_SELECTOR, "[data-price-amount]")
        if inner:
            amt = inner.get_attribute("data-price-amount") or ""
    # 그래도 없으면 텍스트에서 숫자 추출
    if not amt:
        amt = _extract_primary_price(el.text or "")
    return amt

def _extract_old_price(item) -> str:
    # rrpPrice의 data-price-amount가 정가
    old_amt = _extract_price_amount(item, "rrpPrice")
    return old_amt if old_amt else ""


def parse_list_grid_products(products: Iterable, start_rank: int = 1):
    """?? ??(li.product-item) ????? ??/???/??? ??."""
    names, prices, old_prices, ranks, brands = [], [], [], [], []
    rank = start_rank

    for item in products:
        try:
            data_name = _safe_find_attr(item, "div.product-item-info", "data-name")
            if not data_name:
                data_name = _safe_find_attr(item, "a.product-item-link", "title") or _safe_find_text(item, "a.product-item-link")

            normalized_name = _normalize_product_name(data_name or "N/A")

            brand = "N/A"
            product = normalized_name
            if "-" in normalized_name:
                brand_part, product_part = normalized_name.split("-", 1)
                brand_candidate = brand_part.strip()
                product = _normalize_product_name(product_part)
                brand = brand_candidate if brand_candidate else "N/A"

            old_price = _extract_old_price(item)

            final_amount = _extract_price_amount(item, "finalPrice")
            if final_amount:
                price_value = final_amount
            else:
                price_text = (
                    _safe_find_text(item, "[data-price-type='finalPrice'] span.price")
                    or _safe_find_text(item, "span.normal-price span.price-container span.price")
                    or _safe_find_text(item, "span.price")
                )
                price_value = _extract_primary_price(price_text or "N/A")

            names.append(product)
            prices.append(price_value)
            ranks.append(rank)
            brands.append(brand)
            rank += 1
            old_prices.append(old_price)

        except (StaleElementReferenceException, NoSuchElementException):
            names.append("N/A")
            prices.append("N/A")
            old_prices.append("")
            ranks.append(rank)
            brands.append("N/A")
            rank += 1

    return names, prices, old_prices, ranks, brands


def stylevana_scroll_crawling_with_sb(
    sb,
    url: str,
    target_count: int = 100,
    product_selector: str = "#layered-ajax-list-products ol li.product-item",
):
    base_url = url.split("?")[0]

    gathered_names: List[Optional[str]] = []
    gathered_prices: List[Optional[str]] = []
    gathered_old_prices: List[Optional[str]] = []
    gathered_ranks: List[Optional[int]] = []
    gathered_brands: List[Optional[str]] = []

    for page_no in range(1, 10):

        page_url = f"{base_url}?p={page_no}"
        sb.open(page_url)
        try:
            sb.execute_script("document.querySelector('.content-popup')?.remove();")
        except Exception:
            pass

        if page_no == 1:
            time.sleep(5)
        sb.wait_for_ready_state_complete()
        sb.wait_for_element_present(product_selector, timeout=20)
        time.sleep(0.5)

        products = sb.find_elements(product_selector)
        if not products:
            print(f"[WARN] No products found with selector '{product_selector}' at {page_url}.")
            break

        remaining = target_count - len(gathered_names)
        batch = products[:remaining]

        names, prices, old_prices, ranks, brands = parse_list_grid_products(
            batch, start_rank=len(gathered_names) + 1
        )

        gathered_names.extend(names)
        gathered_prices.extend(prices)
        gathered_old_prices.extend(old_prices)
        gathered_ranks.extend(ranks)
        gathered_brands.extend(brands)

        if len(gathered_names) >= target_count:
            break

    fill_missing(gathered_names, target_count, "N/A")
    fill_missing(gathered_prices, target_count, "N/A")
    fill_missing(gathered_old_prices, target_count, "")
    fill_missing(gathered_ranks, target_count, None)
    fill_missing(gathered_brands, target_count, "N/A")

    return gathered_names, gathered_prices, gathered_old_prices, gathered_ranks, gathered_brands
def stylevana_scroll_crawling_with_sb(
    sb,
    url: str,
    target_count: int = 100,
    product_selector: str = "#layered-ajax-list-products ol li.product-item",
):
    base_url = url.split("?")[0]

    gathered_names: List[Optional[str]] = []
    gathered_prices: List[Optional[str]] = []
    gathered_old_prices: List[Optional[str]] = []
    gathered_ranks: List[Optional[int]] = []
    gathered_brands: List[Optional[str]] = []

    for page_no in range(1, 10):

        page_url = f"{base_url}?p={page_no}"
        sb.open(page_url)

        if page_no == 1:
            time.sleep(5)
        # ✅ 단순 sleep 대신: DOM + 상품 카드 로딩을 명시적으로 기다림
        sb.wait_for_ready_state_complete()
        sb.wait_for_element_present(product_selector, timeout=20)
        time.sleep(0.5)  # 살짝만(가격 렌더링 안정화)

        products = sb.find_elements(product_selector)
        if not products:
            print(f"[WARN] No products found with selector '{product_selector}' at {page_url}.")
            break

        remaining = target_count - len(gathered_names)
        batch = products[:remaining]

        names, prices, old_prices, ranks, brands = parse_list_grid_products(
            batch, start_rank=len(gathered_names) + 1
        )

        gathered_names.extend(names)
        gathered_prices.extend(prices)
        gathered_old_prices.extend(old_prices)
        gathered_ranks.extend(ranks)
        gathered_brands.extend(brands)

        if len(gathered_names) >= target_count:
            break

    fill_missing(gathered_names, target_count, "N/A")
    fill_missing(gathered_prices, target_count, "N/A")
    fill_missing(gathered_old_prices, target_count, "")
    fill_missing(gathered_ranks, target_count, None)
    fill_missing(gathered_brands, target_count, "N/A")

    return gathered_names, gathered_prices, gathered_old_prices, gathered_ranks, gathered_brands

# --------------------------------------------------------------------------- #
# DB 관련 함수
#  - 스키마/테이블 생성, INSERT 헬퍼
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


def ensure_stylevana_table_exists(connection) -> None:
    """
    suncream_crawling 스키마와 stylevana 테이블이 없으면 생성합니다.
    """
    cursor = connection.cursor()
    cursor.execute("CREATE SCHEMA IF NOT EXISTS suncream_crawling;")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS suncream_crawling.stylevana (
            id BIGSERIAL PRIMARY KEY,
            "Rank" INTEGER,
            "Brand" VARCHAR(255),
            "Product" TEXT,
            "Old_price" VARCHAR(50),
            "Price" VARCHAR(50),
            "DateTime" TIMESTAMP,
            "Channel" VARCHAR(50),
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
        print("DB 저장 비활성화(stylevana_USE_DB=false); 저장을 건너뜁니다.")
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
        ensure_stylevana_table_exists(connection)
        cursor = connection.cursor()
        query = """
            INSERT INTO suncream_crawling.stylevana
                ("Rank", "Brand", "Product", "Old_price","Price", "DateTime", "Channel")
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(query, rows)
        connection.commit()
        cursor.close()
        print(f"Inserted {len(rows)} rows into suncream_crawling.stylevana.")
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
    """
    크롤링 → 전처리 → DB 저장 → 엑셀 생성 → 메일 발송의 전체 흐름을 수행합니다.
    실행 순서:
      1) SeleniumBase로 카테고리별 상품 크롤링
      2) DataFrame 정제/중복 제거
      3) DB 저장 (환경변수로 USE_DB 제어)
      4) 엑셀 리포트 생성 및 이메일 발송
    """
    run_time = datetime.now(SEOUL_TZ)
    run_time_naive = run_time.astimezone(SEOUL_TZ).replace(tzinfo=None)
    iso_timestamp = run_time.strftime("%Y-%m-%d %H:%M:%S")

    category_configs = [
        {"name": "Bestsellers", "url": BESTSELLER_URL, "limit": CRAWL_LIMIT},
    ]

    category_frames: dict[str, pd.DataFrame] = {}

    with SB(uc=True, headless=False) as sb:
        sb.driver.maximize_window()
        for config in category_configs:
            print(f"Start crawling {config['name']}...")
            names, prices, old_prices, ranks, brands = stylevana_scroll_crawling_with_sb(
                sb,
                config["url"],
                target_count=config["limit"],
            )
            df = pd.DataFrame(
                {
                    COL_RANK: ranks,
                    COL_BRAND: brands,
                    COL_PRODUCT_NAME: names,
                    COL_PRICE: prices,
                    COL_OLD_PRICE: old_prices,
                    COL_DATETIME_TEXT: iso_timestamp,
                    COL_CHANNEL: ["stylevana"] * len(names),
                }
            )
            df[COL_COLLECTED_AT] = iso_timestamp
            _add_match_keys_inplace(df)
            category_frames[config["name"]] = df.dropna(subset=[COL_RANK, COL_PRODUCT_NAME]).copy()

    for df in category_frames.values():
        df[COL_RANK] = pd.to_numeric(df[COL_RANK], errors="coerce")
        df.dropna(subset=[COL_RANK, COL_PRODUCT_NAME], inplace=True)
        df[COL_RANK] = df[COL_RANK].astype(int)
        for col in (COL_BRAND, COL_PRICE, COL_OLD_PRICE, COL_CHANNEL):
            if col in df.columns:
                df[col] = df[col].fillna("")
        df.sort_values(by=COL_RANK, inplace=True)
        df.reset_index(drop=True, inplace=True)
        _add_match_keys_inplace(df)

    combined_df_all = pd.concat(category_frames.values(), ignore_index=True)

    db_rows = []
    for _, row in combined_df_all.iterrows():
        price_value = row[COL_PRICE]
        old_price_value = row.get(COL_OLD_PRICE, "")
        # Old price가 비었거나 N/A면 현재 판매가로 대체해서 DB에 넣는다.
        if pd.isna(old_price_value) or str(old_price_value).strip() in ("", "N/A"):
            old_price_value = price_value

        db_rows.append(
            (
                int(row[COL_RANK]),
                row[COL_BRAND],
                row[COL_PRODUCT_NAME],
                old_price_value,
                price_value,
                row[COL_DATETIME_TEXT],
                row[COL_CHANNEL],
            )
        )
    insert_into_postgresql(db_rows)

    print("StyleVana 크롤링 완료.")


if __name__ == "__main__":
    main()
