from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import requests
import pandas as pd


def Scrap_narasite(file_path, status) :
    df = pd.DataFrame(columns=['title', 'price', 'date', 'location', 'content', 'img_url', 'post_url'])
    with open(file_path, 'r') as f:
        for url in f:
            try:
                driver = webdriver.Chrome()
                driver.get(url)
                time.sleep(1)
                # 내부 정보 먼저확인 삭제된 게시글일시 Error 캐치후 다음 URL 진행
                content = driver.find_element(By.TAG_NAME, 'article').text
                # 판매자가 거래희망지역 설정한 경우 내부 content에 거래희망지역 문자열 존재 거래희망지역 기준으로 제품 정보와 지역정보 분할
                if "거래희망지역" in content:
                        content = content.split("거래희망지역")
                        location = content[1]
                        content = content[0]
                else:
                        location = None

                #title_tag 찾고 parent, parent 태그 찾으면 title, price, date 정보를 갖고있는 tag가 선택됨
                title_tag = driver.find_element(By.TAG_NAME, 'h1')
                complex = driver.execute_script("return arguments[0].parentNode;", title_tag)
                complex = driver.execute_script("return arguments[0].parentNode;", complex)
                complex = complex.text.split('\n')
                title = complex[0]
                price = complex[1].replace("원", '').replace(",", '')
                date = complex[2][:(complex[2].find('·'))-1].strip()
                img_url = driver.find_element(By.CLASS_NAME, 'col-span-1').find_element(By.TAG_NAME, 'img').get_attribute("src")
                post_url = url
                sample = pd.DataFrame([{'title':title, 'price': price, 'date':date, 'location': location, 'content':content, 'img_url':img_url, 'post_url':post_url, 'status':status}])
                df = pd.concat([df,sample], ignore_index=True)
                print(df)
            except Exception as e:
                print(f"Error occur:{e}")
                continue
    df.to_csv(file_path + "Scraping.csv", index=False)

if __name__ == "__main__":
    file_path = 'selling_URL/selling_url_300000-1000000.txt'
    #soldout 링크 스크랩핑이면 1, selling scraping이면 0
    status = 0
    Scrap_narasite(file_path, status)




 



# https://web.joongna.com/product/168060749 삭제된 게시글