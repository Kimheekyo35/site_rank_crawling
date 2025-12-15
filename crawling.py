import time
import pandas as pd
import selenium.webdriver as webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

options = Options()
options.add_argument("--window-size=1920,1080")

# Use Service so ChromeDriver is picked up reliably on Windows.
driver = webdriver.Chrome(options=options)

rank_dict = {}
item_name_list = []
rank = 1

for page in range(1, 10):
    url = f"https://www.stylevana.com/en_US/best-sellers.html?p={page}"
    driver.get(url)
    time.sleep(2)
    
    # page가 1일 때만 팝업창 때문에 새로고침
    if page == 1:
        driver.refresh()
        time.sleep(2)

    # driver.execute_script("document.body.style.zoom='50%'")
    # time.sleep(2)
    # driver.execute_script("window.scrollTo(0,200)")
    # time.sleep(1)

    # 12개씩 가져오기
    for i in range(1,13):
        name = driver.find_element(By.CSS_SELECTOR,f"#layered-ajax-list-products > div > div.products.wrapper.grid.products-grid > div > ol > li:nth-child({i}) > div > div > div.product-info > div > strong > a").text
        item_name_list.append(name)
    time.sleep(1)
driver.quit()

# 리스트로 모은 다음 한번에 ranking 부여
for name in item_name_list:
    if not name:
        continue
    rank_dict[rank] = name
    rank += 1
    if rank == 101:
        break

# df = pd.DataFrame(
#     sorted(rank_dict.items()),
#     columns=['랭킹','상품명']
# )

# 엑셀 이름에 날짜 설정
year = time.localtime().tm_year
month = time.localtime().tm_mon
day = time.localtime().tm_mday

# 엑셀 저장
pd.DataFrame(sorted(rank_dict.items()),columns=['랭킹','상품명']).to_excel(f"{year}_{month}_{day}_stylevana_ranking_1_100.xlsx",index=False)

