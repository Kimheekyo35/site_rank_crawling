import os
import time
import re
import random
import tempfile
import shutil
from pathlib import Path
from typing import Optional, Iterable, Tuple, List
from selenium.webdriver.chrome.options import Options
from selenium import webdriver
import pandas as pd
import psycopg2
from psycopg2 import Error
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import smtplib
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from dotenv import load_dotenv

# ======postgreSQL 연결 ==========
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
COL_DATETIME_TEXT = "Datetime"
COL_CHANNEL = "Channel"
COL_OLD_PRICE = "Old_Price"

COL_COLLECTED_AT = "수집일시"
COL_PREVIOUS_RANK = "전일 순위"
COL_RANK_DELTA = "전일 변동"
COL_STATUS = "전일대비 증감"
COL_PREVIOUS_RANK_WEEK = "전주 순위"
COL_WEEK_DELTA = "전주 변동"
COL_WEEK_STATUS = "전주대비 증감"

options = Options()
options.add_argument("--window-size=1920,1080")

# Use Service so ChromeDriver is picked up reliably on Windows.
driver = webdriver.Chrome(options=options)

DEFAULT_BESTSELLER_URL="https://jolse.com/category/suncare/1032/?cate_no=1032&sort_method=6&"
BESTSELLER_URL = os.getenv("JOLSE_BESTSELLER_URL", DEFAULT_BESTSELLER_URL)

CRAWL_LIMIT = 100
REPORT_LIMIT = 50
COL_BRAND_KEY = "_brand_key"
COL_PRODUCT_KEY = "_product_key"

# data 저장 path
def ensure_data_directory() -> Path:
    base = Path(__file__).resolve().parent
    data_dir = base / "data"
    data_dir.mkdir(exist_ok=True)
    return data_dir

# DB 관련 함수
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

# sql 스키마 생성
def ensure_jolse_table_exists(connection) -> None:
    """
    suncream_crawling 스키마와 jolse 테이블이 없으면 생성합니다.
    """
    cursor = connection.cursor()
    cursor.execute("CREATE SCHEMA IF NOT EXISTS suncream_crawling;")
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS suncream_crawling.jolse (
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

# postgresql에 저장
def insert_into_postgresql(rows: List[Tuple]):
    if not rows:
        return
    if not USE_DB:
        print("DB 저장 비활성화(JOLSE_USE_DB=false); 저장을 건너뜁니다.")
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
        ensure_jolse_table_exists(connection)
        cursor = connection.cursor()
        query = """
            INSERT INTO suncream_crawling.jolse
                ("Rank", "Brand", "Product", "Old_price", "Price", "DateTime", "Channel")
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.executemany(query, rows)
        connection.commit()
        cursor.close()
        print(f"Inserted {len(rows)} rows into suncream_crawling.jolse.")
    except Error as exc:
        if connection:
            connection.rollback()
        print(f"PostgreSQL insert error: {exc}")
    finally:
        if connection:
            connection.close()

def delete_query(rows: List[Tuple]):
    if not rows:
        return
    if not USE_DB:
        print("DB 저장 비활성화(JOLSE_USE_DB=false); 저장을 건너뜁니다.")
        return
    connection = None
    try:
        _ensure_empty_pgpass()
        connection = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_DATABASE,
            user=DB_USER,
            password=DB_PASSWORD
        )
        ensure_jolse_table_exists(connection)
        cursor = connection.cursor()
        query = """
            DELETE FROM suncream_crawling.jolse
            WHERE id NOT IN (
                SELECT MIN(id)
                FROM suncream_crawling.jolse
                GROUP BY "Brand", "Product", "Old_price", "Price", "DateTime"
            );
        """
        cursor.executemany(query,rows)
        connection.commit()
        cursor.close()
        print(f"Deleted {len(rows)} rows into suncream_crawling.jolse.")
    except Error as exc:
        if connection:
            connection.rollback()
        print(f"PostgreSQL insert error: {exc}")
    finally:
        if connection:
            connection.close()

# 각 상품의 브랜드, 가격, 할인 전 가격, 제품명 가져오기
def parse_jolse_product_detail(
    driver: webdriver.Chrome,
    wait: WebDriverWait,
) -> Tuple[str, str, str, str]:
    """
    return: (brand, product_name, price_discounted, original_price)
    """
    detail = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#detail")))

    # 1) brand
    brand = detail.find_element(By.CSS_SELECTOR, "tr.prd_brand_css td span").text.strip()

    product = detail.find_element(By.CSS_SELECTOR,"#totalProducts table td").text

    price_discounted = re.search(
        r"\d+(\.\d+)?",
        detail.find_element(By.CSS_SELECTOR, "#span_product_price_sale").text
    ).group()

    original_price = re.search(
        r"\d+(\.\d+)?",
        detail.find_element(By.CSS_SELECTOR, "#span_product_price_text").text
    ).group()

    return brand, product, price_discounted, original_price


# 각 페이지 안으로 들어가서 id 뽑아오고 list에 제품명 등등 넣기
def jolse_category_crawling(
    driver: webdriver.Chrome,
    url_prefix: str,
    target_count: int = 100,
    list_selector: str = "ul.prdList.grid5 li",
    detail_url_template: str = "https://jolse.com/product/skin1004-madagascar-centella-hyalu-cica-water-fit-sun-serum-dual-pack/{pid}/category/1032/display/1/",
    wait_sec: int = 8,
    max_pages: int = 3,
) -> Tuple[
    List[Optional[int]],
    List[Optional[str]],
    List[Optional[str]],
    List[Optional[str]],
    List[Optional[str]],
]:
    """
    returns: (ranks, brands, products, discounted_prices, original_prices)
      - products: brand 제거한 상품명(원하면 아래 한 줄 주석 바꾸면 됨)
      - prices: 할인 가격(없으면 "")
      - ranks: 1..target_count
      - brands: 브랜드
    """
    wait = WebDriverWait(driver, wait_sec)
    gathered_products: List[Optional[str]] = []
    gathered_discounted_prices: List[Optional[str]] = []
    gathered_original_prices: List[Optional[str]] = []
    gathered_ranks: List[Optional[int]] = []
    gathered_brands: List[Optional[str]] = []

    # 1) 카테고리 페이지에서 pid 수집
    id_list: List[str] = []
    for page_no in range(1, max_pages + 1):

        if len(id_list) >= target_count:
            break
# https://jolse.com/category/suncare/1032/?cate_no=1032&sort_method=6&page=1
        page_url = f"{url_prefix}page={page_no}"
        driver.get(page_url)
        time.sleep(random.uniform(1.0, 1.8))

        try:
            wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, list_selector)))
        except Exception:
            pass

        li_elements = driver.find_elements(By.CSS_SELECTOR, list_selector)
        if not li_elements:
            break

        for li in li_elements:
            li_id = li.get_attribute("id")  # e.g. "anchorBoxId_68608"
            if not li_id:
                continue
            m = re.search(r"\d+", li_id)
            if not m:
                continue
            pid = m.group()
            id_list.append(pid)
            if len(id_list) >= target_count:
                break

    # 2) 상세 페이지 들어가서 brand/product/price 파싱
    rank = 0
    for pid in id_list:
        rank += 1
        each_url = detail_url_template.format(pid=pid)
        driver.get(each_url)
        time.sleep(random.uniform(0.6, 1.2))

        try:
            brand, product, price_discounted, original_price = parse_jolse_product_detail(driver, wait)
        except Exception:
            brand, product, price_discounted, original_price = "", "", "", ""

        gathered_brands.append(brand or "")
        gathered_products.append(product or "")
        gathered_discounted_prices.append(price_discounted or "")
        gathered_original_prices.append(original_price or "")
        gathered_ranks.append(rank)

        if rank >= target_count:
            break

    return gathered_ranks, gathered_brands, gathered_products, gathered_discounted_prices, gathered_original_prices


def main():
    """
    크롤링 → 전처리 → DB 저장 순서로 진행
    """
    run_time = datetime.now(SEOUL_TZ)
    iso_timestamp = run_time.strftime("%Y-%m-%d %H:%M:%S")

    category_configs = [{"name": "Bestsellers", "url_prefix": BESTSELLER_URL, "limit": CRAWL_LIMIT}]

    for config in category_configs:
        ranks, brands, products, prices_discounted, original_prices = jolse_category_crawling(
            driver,
            config["url_prefix"],
            target_count=config["limit"],
        )
        df = pd.DataFrame(
            {
                COL_RANK: ranks,
                COL_BRAND: brands,
                COL_PRODUCT_NAME: products,
                COL_PRICE: prices_discounted,
                COL_OLD_PRICE: original_prices,
                COL_DATETIME_TEXT: iso_timestamp,
                COL_CHANNEL: ["jolse"] * len(ranks),
            }
        )
    df[COL_COLLECTED_AT] = iso_timestamp
    df[COL_RANK] = pd.to_numeric(df[COL_RANK], errors="coerce")
    df.dropna(subset=[COL_RANK, COL_PRODUCT_NAME], inplace=True)
    df[COL_RANK] = df[COL_RANK].astype(int)
    for col in (COL_BRAND, COL_PRICE, COL_OLD_PRICE, COL_CHANNEL):
        if col in df.columns:
            df[col] = df[col].fillna("")
    df.sort_values(by=COL_RANK, inplace=True)
    df.reset_index(drop=True, inplace=True)

    db_rows: List[Tuple] = []
    for row in df.itertuples(index=False):
        price_value = row.Price
        old_price_value = row.Old_Price
        # Old price가 비었거나 N/A면 현재 판매가로 대체해서 DB에 넣는다.
        if pd.isna(old_price_value) or str(old_price_value).strip() in ("", "N/A"):
            old_price_value = price_value
        db_rows.append(
            (
                row.Rank,
                row.Brand,
                row.Product,
                old_price_value,
                price_value,
                row.Datetime,
                row.Channel,
            )
        )
    insert_into_postgresql(db_rows)
    delete_query(db_rows)

    ensure_data_directory()
    print("jolse 크롤링 완료.")


if __name__ == "__main__":
    main()
    