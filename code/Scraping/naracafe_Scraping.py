from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time

# Chrome driver 옵션 설정
chrome_options = Options()
chrome_options.add_argument("--headless")  # GUI 없이 실행
chrome_options.add_argument("--no-sandbox")  # 샌드박스 모드 비활성화
chrome_options.add_argument("--disable-dev-shm-usage")  # /dev/shm 사용 비활성화
chrome_options.add_argument("--log-level=3") # 로그 수준을 낮춰 warning message 출력 제한 


def get_postInfo(file_path, status):
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
  
    # file_path에 있는 모든 Url 읽기
    with open(file_path, 'r') as f:
        urls = [url.strip() for url in f]

    # Url 500개씩 처리하기 위해 batch_size, max_index 설정
    batch_size = 500
    max_index = (len(urls) + batch_size - 1) // batch_size

    # batch_size와 index를 이용하여 추출할 Url 500개 범위 지정
    for index in range(0, max_index):
        start_index = index * batch_size
        end_index = min(start_index + batch_size, len(urls))
        sub_urls = urls[start_index:end_index]

        post_df = pd.DataFrame(columns=['title', 'context', 'price', 'upload_date', 'location', 'status', 'imgUrl', 'url'])

        for url in sub_urls:
            try:
                driver.get(url)

                # iframe 로드 될때까지 기다림
                iframe_element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "cafe_main")))
                driver.switch_to.frame(iframe_element)

                # post_info 추출
                title_element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'title_text')))
                title = title_element.text
                upload_date = driver.find_element(By.CLASS_NAME, 'date').text
                price = int(driver.find_element(By.CLASS_NAME, "cost").text.replace('원','').replace(',',''))
                imgUrl = driver.find_element(By.CLASS_NAME, 'image').get_attribute('src')
                components = driver.find_elements(By.CLASS_NAME, 'se-component-content')
                context = ''
                for component in components:
                    p_tags = component.find_elements(By.TAG_NAME, "p")
                    for p_tag in p_tags:
                        context += p_tag.text

                # post_info를 하나의 sample로 변경
                sample = pd.DataFrame([{"title":title, "context":context, "price":price, "upload_date":upload_date, "location":None, 'status':status, 'imgUrl':imgUrl, 'url':url}])
                post_df = pd.concat([post_df, sample], ignore_index=True)

            except Exception as e:
                print(f"{e}\nRestart after 2 seconds")
                time.sleep(2)
                continue

            # post_info가 정상적으로 추출됨을 알림
            print('.', end='', flush=True)

        # 500개 정상 추출시 csv로 저장
        output_file_path = file_path.replace('Url', 'Post').replace('.txt', '.csv')
        try:
            df = pd.read_csv(output_file_path)
            df = pd.concat([df, post_df], ignore_index=True)
        except FileNotFoundError:
            df = post_df
        
        df.to_csv(output_file_path, index=False, encoding='utf-8-sig')

if __name__ == "__main__" :
    status_list = ['selling', 'soldout']

    # 추출할 Url 담긴 text파일 path 설정
    # 중고나라 사이트인지 카페인지 주의깊게 확인
    file_path = "soldout/iphone14_soldout_Url_cafe.txt"

    # Url들이 판매된것인지 판매중인지 입력
    get_postInfo(file_path, 'soldout')


            



