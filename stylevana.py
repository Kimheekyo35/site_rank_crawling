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

# 리스트 길이를 최소 target까지 맞추는 보조 함수. target보다 리스트의 길이가 짧으면 None을 채워줌
# def fill_missing(items: List[Optional[str]], target: int) -> List[Optional[str]]:
#     if len(items) < target:
#         items.extend([None] * (target - len(items)))
#     return items

# def _extract_primary_price(text: str) -> str:
#     """문자열에서 통화 기호와 숫자만 깔끔히 뽑아냅니다."""
#     if not text:
#         return "N/A"
#     match = re.search(r"([₩￦$€£¥])\s?\d[\d,]*(?:\.\d+)?", text)
#     if match:
#         return match.group(0).strip()
#     num = re.search(r"\d[\d,]*(?:\.\d+)?", text)
#     if num:
#         prefix = text[:num.start()].strip()
#         symbol = ""
#         if prefix and prefix[-1] in "₩￦$€£¥":
#             symbol = prefix[-1]
#         return f"{symbol} {num.group(0)}".strip()
#     return text.strip()
        

# def _extract_text_with_fallback(item, selectors: List[str]) -> Optional[str]:
#     """지정된 CSS 셀렉터 목록에서 텍스트를 찾는 즉시 반환합니다."""
#     for selector in selectors:
#         try:
#             element = item.find_element(By.CSS_SELECTOR, selector)
#             text = element.text.strip()
#             if text:
#                 return text
#         except Exception:
#             continue
#     return None


# def _extract_old_price(item) -> Optional[str]:
#     """상품 카드 내부에서 정가(취소선 가격)를 추출합니다. 없으면 N/A."""
#     selector = "span.rrp-old-price span.price"
#         # "span[class*='rrp-old-price'] 
#     text = _extract_text_with_fallback(item, selector)
#     if not text:
#         return "N/A"
#     return _extract_primary_price(text)




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
    stylevana 스키마와 stylevana 테이블이 없으면 생성합니다.
    """
    cursor = connection.cursor()
    cursor.execute("CREATE SCHEMA IF NOT EXISTS suncream_crawling;")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS stylesuncream_crawling.stylevana (
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
# 데이터 가공
# --------------------------------------------------------------------------- #
def _format_status_from_delta(delta_value) -> str:
    if pd.isna(delta_value):
        return "신규"
    if delta_value > 0:
        return f"▲{int(delta_value)}"
    if delta_value < 0:
        return f"▼{abs(int(delta_value))}"
    return "-"


def _preprocess_rank_dataframe(df: Optional[pd.DataFrame]) -> Optional[pd.DataFrame]:
    """순위/제품명/브랜드 등 텍스트 정제 및 숫자 캐스팅을 수행합니다."""
    if df is None:
        return None
    work = df.copy()
    if COL_PRODUCT_NAME in work.columns:
        work = work[work[COL_PRODUCT_NAME].notna()]
        work[COL_PRODUCT_NAME] = work[COL_PRODUCT_NAME].apply(_normalize_product_name)
    if COL_RANK in work.columns:
        work[COL_RANK] = pd.to_numeric(work[COL_RANK], errors="coerce")
        work = work[work[COL_RANK].notna()]
        work[COL_RANK] = work[COL_RANK].astype(int)
    for col in (COL_BRAND, COL_CHANNEL):
        if col in work.columns:
            work[col] = (
                work[col]
                .fillna("")
                .apply(lambda x: re.sub(r"\s+", " ", str(x)).strip())
            )
    _add_match_keys_inplace(work)
    return work


def annotate_rank_changes(
    current_df: pd.DataFrame,
    previous_df: Optional[pd.DataFrame],
) -> Tuple[pd.DataFrame, Optional[pd.DataFrame], Optional[str]]:
    """전일 데이터와 비교해 순위 변동/차트아웃을 계산합니다."""
    current_clean = _preprocess_rank_dataframe(current_df)
    previous_clean = _preprocess_rank_dataframe(previous_df)

    if previous_clean is None or previous_clean.empty:
        annotated = current_clean.copy()
        annotated[COL_PREVIOUS_RANK] = pd.NA
        annotated[COL_RANK_DELTA] = pd.NA
        annotated[COL_STATUS] = "신규"
        annotated.sort_values(by=[COL_RANK], inplace=True)
        return annotated, None, None

    keys = [COL_CHANNEL, COL_BRAND_KEY, COL_PRODUCT_KEY]
    merged = current_clean.merge(
        previous_clean[keys + [COL_RANK]],
        how="left",
        on=keys,
        suffixes=("", "_previous"),
    )

    merged.rename(columns={f"{COL_RANK}_previous": COL_PREVIOUS_RANK}, inplace=True)
    merged[COL_PREVIOUS_RANK] = pd.to_numeric(merged[COL_PREVIOUS_RANK], errors="coerce").astype("Int64")
    merged[COL_RANK_DELTA] = merged[COL_PREVIOUS_RANK] - merged[COL_RANK]
    merged[COL_RANK_DELTA] = pd.to_numeric(merged[COL_RANK_DELTA], errors="coerce").astype("Int64")

    merged[COL_STATUS] = merged[COL_RANK_DELTA].apply(_format_status_from_delta)

    dropped = previous_clean.merge(
        merged[keys],
        how="left",
        on=keys,
        indicator=True,
    )
    dropped = dropped[dropped["_merge"] == "left_only"].drop(columns=["_merge"])
    if dropped.empty:
        dropped_df = None
    else:
        dropped_df = dropped.sort_values(by=[COL_RANK]).copy()
        dropped_df[COL_PREVIOUS_RANK] = dropped_df[COL_RANK]
        dropped_df.drop(columns=[COL_RANK], inplace=True)
        dropped_df[COL_STATUS] = "차트 아웃"

    previous_label = None
    if previous_df is not None and COL_DATETIME_TEXT in previous_df.columns:
        series = previous_df[COL_DATETIME_TEXT].dropna()
        if not series.empty:
            previous_label = series.iloc[0]

    merged.sort_values(by=[COL_RANK], inplace=True)
    return merged, dropped_df, previous_label


def add_weekly_rank_changes(
    current_df: pd.DataFrame,
    previous_week_df: Optional[pd.DataFrame],
) -> Tuple[pd.DataFrame, Optional[str]]:
    """전주 데이터와 비교해 주간 변동을 계산합니다."""
    if previous_week_df is None or previous_week_df.empty:
        df = current_df.copy()
        df[COL_PREVIOUS_RANK_WEEK] = pd.NA
        df[COL_WEEK_DELTA] = pd.NA
        df[COL_WEEK_STATUS] = "-"
        return df, None

    prev_clean = _preprocess_rank_dataframe(previous_week_df)
    if prev_clean is None or prev_clean.empty:
        df = current_df.copy()
        df[COL_PREVIOUS_RANK_WEEK] = pd.NA
        df[COL_WEEK_DELTA] = pd.NA
        df[COL_WEEK_STATUS] = "-"
        return df, None

    keys = [COL_CHANNEL, COL_BRAND_KEY, COL_PRODUCT_KEY]
    merged = current_df.merge(
        prev_clean[keys + [COL_RANK]],
        how="left",
        on=keys,
        suffixes=("", "_week"),
    )

    merged.rename(columns={f"{COL_RANK}_week": COL_PREVIOUS_RANK_WEEK}, inplace=True)
    merged[COL_PREVIOUS_RANK_WEEK] = pd.to_numeric(merged[COL_PREVIOUS_RANK_WEEK], errors="coerce").astype("Int64")
    merged[COL_WEEK_DELTA] = merged[COL_PREVIOUS_RANK_WEEK] - merged[COL_RANK]
    merged[COL_WEEK_DELTA] = pd.to_numeric(merged[COL_WEEK_DELTA], errors="coerce").astype("Int64")
    merged[COL_WEEK_STATUS] = merged[COL_WEEK_DELTA].apply(_format_status_from_delta)

    week_label = None
    if previous_week_df is not None and COL_DATETIME_TEXT in previous_week_df.columns:
        series = previous_week_df[COL_DATETIME_TEXT].dropna()
        if not series.empty:
            week_label = series.iloc[0]

    merged.sort_values(by=[COL_RANK], inplace=True)
    return merged, week_label


def summarize_changes_for_email(run_time: datetime, best_df: pd.DataFrame) -> str:
    """상위 5개 상품을 요약해 이메일 본문 텍스트로 만듭니다."""
    lines: List[str] = [
        "안녕하세요. 스타일바나 선크림 수집 결과입니다.",
        f"수집 시각 (KST): {run_time.strftime('%Y-%m-%d %H:%M')}",
        "",
        "상위 5개 상품:",
    ]

    top_rows = (
        best_df.copy()
        .dropna(subset=[COL_RANK])
        .sort_values(by=COL_RANK)
        .head(5)
    )
    for _, row in top_rows.iterrows():
        lines.append(
            f"  #{int(row[COL_RANK])} {row.get(COL_BRAND, '')} - {row.get(COL_PRODUCT_NAME, '')} ({row.get(COL_PRICE, '')})"
        )

    lines.append("")
    lines.append("첨부된 엑셀 파일에서 상세 내용을 확인해주세요.")
    lines.append("감사합니다.")
    return "\n".join(lines)

def _format_korean_time(dt: datetime) -> str:
    hour = dt.hour
    minute = dt.minute

    if hour == 0:
        period = "오전"
        display_hour = 12
    elif 1 <= hour < 12:
        period = "오전"
        display_hour = hour
    elif hour == 12:
        period = "오후"
        display_hour = 12
    else:
        period = "오후"
        display_hour = hour - 12

    if minute:
        return f"{period} {display_hour}시 {minute:02d}분"
    return f"{period} {display_hour}시"


def save_excel_report(
    report_path: Path,
    df: pd.DataFrame,
    run_time: datetime,
    top_n: int = REPORT_LIMIT,
) -> None:
    """베스트셀러 표를 엑셀 시트로 저장합니다. (현재/정가/브랜드/랭킹만 표시, 변동치는 제외)"""
    report_df = (
        df.copy()
        .dropna(subset=[COL_RANK])
        .sort_values(by=[COL_RANK])

    )
    # 이전 기준은 category였는데 category를 제거하면 groupby를 할 필요가 없어짐
    report_df = report_df.head(top_n)
    display_df = report_df[
        [
            COL_RANK,
            COL_BRAND,
            COL_PRODUCT_NAME,
            COL_PRICE,
        ]
    ].copy()
    display_df.rename(
        columns={
            COL_RANK: "랭킹",
            COL_BRAND: "브랜드",
            COL_PRODUCT_NAME: "제품명",
            COL_PRICE: "할인가",
        },
        inplace=True,
    )
    display_df["비고"] = ""

    sheet_name = "Bestsellers"
    start_row = 5
    start_col = 3

    with pd.ExcelWriter(report_path, engine="openpyxl") as writer:
        display_df.to_excel(
            writer,
            index=False,
            sheet_name=sheet_name,
            startrow=start_row,
            startcol=start_col,
        )

        worksheet = writer.sheets[sheet_name]
        worksheet["D2"] = "■ 스타일바나 베스트셀러"
        worksheet["D2"].font = Font(bold=True)
        worksheet["D2"].alignment = Alignment(horizontal="left")

        worksheet["D4"] = f"- 기준일자 : {run_time.strftime('%m/%d')} ({_format_korean_time(run_time)})"
        worksheet["D4"].alignment = Alignment(horizontal="left")
        header_row = start_row + 1
        header_font = Font(bold=True)
        for cells in worksheet.iter_rows(
            min_row=header_row,
            max_row=header_row,
            min_col=start_col + 1,
            max_col=start_col + len(display_df.columns),
        ):
            for cell in cells:
                cell.font = header_font
                cell.alignment = Alignment(horizontal="center")

        data_row_start = header_row + 1
        for row in worksheet.iter_rows(
            min_row=data_row_start,
            max_row=data_row_start + len(display_df) - 1,
            min_col=start_col + 1,
            max_col=start_col + len(display_df.columns),
        ):
            for idx, cell in enumerate(row):
                if idx == 0:
                    cell.alignment = Alignment(horizontal="center")
                elif idx == 3:
                    cell.alignment = Alignment(horizontal="right")
                else:
                    cell.alignment = Alignment(horizontal="left")

        column_widths = [8, 22, 40, 18, 18]
        for offset, width in enumerate(column_widths):
            worksheet.column_dimensions[get_column_letter(start_col + 1 + offset)].width = width

        worksheet.freeze_panes = f"{get_column_letter(start_col + 1)}{header_row + 1}"

    print(f"엑셀 리포트 저장 완료: {report_path}")


def send_email_from_memory(excel_bytes: bytes, file_name: str, body_text: Optional[str] = None):
    """
    CC/BCC는 단일 환경변수 `stylevana_CC`로 관리.
    - 구분자: , 또는 ;  (공백 허용)
    - 토큰 접두사:
        * 'cc:'  → 참조
        * 'bcc:' → 숨은참조
      접두사가 없으면 기본적으로 CC로 처리.
    예)
      stylevana_CC="cc:lead@domain.com, bcc:boss@domain.com;ops@domain.com"
      → CC=[lead@, ops@], BCC=[boss@]
    """
    def _split(addr_string: Optional[str]) -> List[str]:
        if not addr_string:
            return []
        return [a.strip() for a in re.split(r"[;,]", addr_string) if a.strip()]

    def _parse_stylevana_cc(value: Optional[str]) -> Tuple[List[str], List[str]]:
        cc_list: List[str] = []
        bcc_list: List[str] = []
        for token in _split(value):
            lower = token.lower()
            if lower.startswith("bcc:"):
                addr = token[4:].strip()
                if addr:
                    bcc_list.append(addr)
            elif lower.startswith("cc:"):
                addr = token[3:].strip()
                if addr:
                    cc_list.append(addr)
            else:
                # 접두사 없으면 CC로 간주
                cc_list.append(token)
        # 중복 제거(순서 유지)
        cc_list = list(dict.fromkeys(cc_list))
        bcc_list = list(dict.fromkeys(bcc_list))
        return cc_list, bcc_list

    smtp_server = os.getenv("SMTP_SERVER", "smtp.gmail.com")
    smtp_port = int(os.getenv("SMTP_PORT", "587"))
    sender_email = os.getenv("EMAIL_ADDRESS") or os.getenv("SENDER_EMAIL")
    sender_password = os.getenv("EMAIL_PASSWORD") or os.getenv("SENDER_PASSWORD")

    # 받는 사람(TO) — 기존 변수를 유지
    to_list = _split(
        os.getenv("EMAIL_RECIPIENTS")
        or os.getenv("EMAIL_RECIPIENT")
        or os.getenv("RECIPIENT_EMAIL")
        or "diana0305@wemarketing.co.kr"
    )

    # CC/BCC — 단일 변수 stylevana_CC 지원
    cc_env = os.getenv("stylevana_CC", "")
    cc_list, bcc_list = _parse_stylevana_cc(cc_env)

    if not sender_email or not sender_password or not (to_list or cc_list or bcc_list):
        print("Email credentials/recipients are missing; skipping email send.")
        return

    msg = MIMEMultipart()
    msg["From"] = sender_email
    if to_list:
        msg["To"] = ", ".join(to_list)
    if cc_list:
        msg["Cc"] = ", ".join(cc_list)
    subject_date = datetime.now(SEOUL_TZ).strftime("%Y%m%d")  # KST 기준 오늘 날짜
    msg["Subject"] = f"예스스타일 랭킹 리포트_{subject_date}"

    body = body_text or f"stylevana crawling completed.\nPlease see the attached report: {file_name}"
    msg.attach(MIMEText(body, "plain", _charset="utf-8"))

    part = MIMEBase("application", "octet-stream")
    part.set_payload(excel_bytes)
    encoders.encode_base64(part)
    part.add_header("Content-Disposition", f"attachment; filename={file_name}")
    msg.attach(part)

    # 실제 전송 대상: To + Cc + Bcc (중복 제거)
    all_recipients = list(dict.fromkeys([*to_list, *cc_list, *bcc_list]))

    attempts = 2
    for attempt in range(1, attempts + 1):
        try:
            server = smtplib.SMTP(smtp_server, smtp_port, timeout=30)
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, all_recipients, msg.as_string())
            server.quit()
            print(f"Email sent successfully to {', '.join(all_recipients)}.")
            return
        except Exception as exc:
            print(f"Email send failed (attempt {attempt}/{attempts}): {exc}")
            if attempt < attempts:
                print("Retrying in 60 seconds...")
                time.sleep(60)
    print("Giving up on email send after retries.")


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

    data_dir = ensure_data_directory()
    # prev_df, prev_created_at, previous_label = fetch_previous_snapshot_from_db(
    #     "Bestsellers", run_time_naive - timedelta(days=1)
    # )
    # if prev_created_at:
    #     print(f"Loaded previous snapshot: {prev_created_at}")
    # else:
    #     print("No previous snapshot found.")

    # prev_week_df, prev_week_created_at, previous_week_label = fetch_previous_snapshot_from_db(
    #     "Bestsellers", run_time_naive - timedelta(days=7)
    # )
    # if prev_week_created_at:
    #     print(f"Loaded previous week snapshot: {prev_week_created_at}")
    # else:
    #     print("No previous week snapshot found.")

    # if prev_df is not None:
    #     prev_df = _deduplicate_by_match_keys(prev_df, limit=CRAWL_LIMIT)
    # if prev_week_df is not None:
    #     prev_week_df = _deduplicate_by_match_keys(prev_week_df, limit=CRAWL_LIMIT)

    bestsellers_df = category_frames["Bestsellers"]
    dedup_order = bestsellers_df.sort_values([COL_BRAND_KEY, COL_PRODUCT_KEY, COL_RANK])
    dedup_order = dedup_order.drop_duplicates(subset=[COL_BRAND_KEY, COL_PRODUCT_KEY], keep="last")
    bestsellers_df = (
        dedup_order.sort_values(by=COL_RANK)
        .head(CRAWL_LIMIT)
        .reset_index(drop=True)
    )
    category_frames["Bestsellers"] = bestsellers_df.copy()
    # annotated_df, dropped_df, previous_label = annotate_rank_changes(bestsellers_df, prev_df)
    # annotated_df, previous_week_label = add_weekly_rank_changes(annotated_df, prev_week_df)

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

    timestamp = run_time.strftime("%y%m%d_%H%M")
    report_path = data_dir / f"stylevana_beauty_{timestamp}.xlsx"
    save_excel_report(
        report_path,
        bestsellers_df,
        run_time=run_time_naive,
        top_n=REPORT_LIMIT,
    )

    with report_path.open("rb") as fp:
        excel_bytes = fp.read()

    email_body = summarize_changes_for_email(run_time_naive, bestsellers_df)
    send_email_from_memory(excel_bytes, report_path.name, email_body)

    print("StyleVana 크롤링 완료.")


if __name__ == "__main__":
    main()
