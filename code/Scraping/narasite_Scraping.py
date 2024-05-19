from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time

# Chrome 옵션 설정
chrome_options = Options()
chrome_options.add_argument("--headless")  # GUI 없이 실행
chrome_options.add_argument("--no-sandbox")  # 샌드박스 모드 비활성화
chrome_options.add_argument("--disable-dev-shm-usage")  # /dev/shm 사용 비활성화
chrome_options.add_argument("--log-level=3")

def get_postInfo(file_path, status) :
    with open(file_path, 'r') as f:
        urls = [url for url in f]
    max_index = (len(urls) + 499) // 500

    for index in range(0, max_index):
        start_index = index * 500
        end_index = min(start_index + (index + 1) * 500, len(urls))
        sub_urls = urls[start_index:end_index]
        driver = webdriver.Chrome(options=chrome_options)
        post_df = pd.DataFrame(columns=['title', 'context', 'price', 'upload_date', 'location', 'status', 'imgUrl', 'url'])

        for url in sub_urls:
            try:
                driver.get(url)
                #article element 로드 될때까지 기다림
                WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "article")))
                context = driver.find_element(By.TAG_NAME, 'article').text
                # 판매자가 거래희망지역 설정한 경우 내부 content에 거래희망지역 문자열 존재, 거래희망지역 기준으로 context와 location 분할
                if "거래희망지역" in context:
                        context = context.split("거래희망지역")
                        location = context[1]
                        context = context[0]
                else:
                        location = None

                #title_tag 찾고 title_tag의 parent, parent인 element를 찾는다. 이 element가 title, price, date 정보를  전부 포함하고 있다.
                title_tag = driver.find_element(By.TAG_NAME, 'h1')
                complex = driver.execute_script("return arguments[0].parentNode;", title_tag)
                complex = driver.execute_script("return arguments[0].parentNode;", complex)
                complex = complex.text.split('\n')
                title = complex[0]
                price = int(complex[1].replace("원", '').replace(",", ''))
                upload_date = complex[2][:(complex[2].find('·'))-1].strip()
                imgUrl = driver.find_element(By.CLASS_NAME, 'col-span-1').find_element(By.TAG_NAME, 'img').get_attribute("src")

                #url 하나에 대한 sample 생성
                sample = pd.DataFrame([{'title':title, 'context':context,'price': price, 'upload_date':upload_date, 'location': location, 'status':status, 'imgUrl':imgUrl, 'url':url}])
                post_df = pd.concat([post_df, sample], ignore_index=True)

            except Exception as e:
                print(f"{e}, Restart after 1.5 seconds")
                time.sleep(1.5)
                continue
            print('.', end='', flush=True)
        
        # 500개 post_info 추출후 csv로 저장
        file_path = file_path.replace('Url', 'Post').replace('.txt', f'{index}.csv') # e.g soldout/iphone14_soldout_Url_site.txt >> soldout/iphone14_soldout_Post_site.csv
        try:
            existing_df = pd.read_csv(file_path)
            combined_df = pd.concat([existing_df, post_df], ignore_index=True)
        except FileNotFoundError:
            combined_df = post_df

        combined_df.to_csv(file_path, index=False)

if __name__ == "__main__":
    status = ['selling', 'soldout']
    #아래 변수 조작 필요
    status = status[1]
    item = "iphone14"
    #
    file_path = f"{status}/{item}_{status}_Url_site.txt"
    get_postInfo(file_path, status)




 



# https://web.joongna.com/product/168060749 삭제된 게시글