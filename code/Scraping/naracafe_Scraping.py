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

def get_postInfo(file_path, status):
    with open(file_path, 'r') as f:
        urls = [url for url in f]
    max_index = len(urls) // 500

    for index in range(1, max_index):
        start_index = 500 * index
        end_index = min(start_index + 500 *(index + 1), len(urls))
        sub_urls = urls[start_index:end_index]
        driver = webdriver.Chrome(options=chrome_options)
        post_df = pd.DataFrame(columns=['title', 'context', 'price', 'upload_date', 'location', 'status', 'imgUrl', 'url'])

        for url in sub_urls:
            try:
                driver.get(url)
                #iframe 로드 될때까지 기다림
                iframe_element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "cafe_main")))
                driver.switch_to.frame(iframe_element)

                # 페이지 내부에서 각 column에 대한 값 추출
                title_element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.CLASS_NAME, 'title_text')))
                title = title_element.text
                upload_date = driver.find_element(By.CLASS_NAME, 'date').text
                price = int(driver.find_element(By.CLASS_NAME, "cost").text.replace('원','').replace(',',''))
                imgUrl = driver.find_element(By.CLASS_NAME, 'image').get_attribute('src')
                # content에 대한 값을 가져오기 위해 componenets > componenet > p.text에 접근해야함
                components = driver.find_elements(By.CLASS_NAME, 'se-component-content')
                context = ''
                for component in components:
                    p_tags = component.find_elements(By.TAG_NAME, "p")
                    for p_tag in p_tags:
                        context += p_tag.text

                # url 하나에 대해서 sample 생성
                sample = pd.DataFrame([{"title" : title, "context" : context, "price" : price, "upload_date" : upload_date, "location" : None, 'status' : status, 'imgUrl' : imgUrl, 'url' : url}])
                post_df = pd.concat([post_df, sample], ignore_index=True)

            except Exception as e:
                print(f"{e}, Restart after 3 seconds")
                time.sleep(1.5)
                continue
            print('.', end='', flush=True)
    
        # 500개 post_info 추출후 csv로 저장
        file_path = file_path.replace('Url', 'Post').replace('.txt', f'{index}.csv') # e.g soldout/iphone14_soldout_Url_cafe.txt >> soldout/iphone14_soldout_Post_cafe.csv
        try:
            existing_df = pd.read_csv(file_path)
            combined_df = pd.concat([existing_df, post_df], ignore_index=True)
        except FileNotFoundError:
            combined_df = post_df
            
        combined_df.to_csv(file_path, index=False)

if __name__ == "__main__" :
    status = ['selling', 'soldout']
    # 아래 변수 설정 필요
    status = status[1]
    item = 'iphone14'
    #
    file_path = f"{status}/{item}_{status}_Url_cafe.txt"
    get_postInfo(file_path, status)


            



