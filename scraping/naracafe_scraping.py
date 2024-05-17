from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import requests
import pandas as pd

def Scrap_naracafe (file_path, status):
    driver = webdriver.Chrome()
    df = pd.DataFrame(columns=['title', 'date', 'price', 'img_url', 'content', 'location', 'status', 'post_url'])
    with open(file_path, 'r') as f:
        error_count = 0
        Scraping_count = 1
        for url in f:
            print(Scraping_count)
            try:
                driver.get(url)
                time.sleep(1) #페이지 열리기 까지 대기 시간
                iframe_element = driver.find_element(By.ID, "cafe_main") 
                driver.switch_to.frame(iframe_element)
                # 페이지 내부에서 각 column에 대한 값 추출
                title = driver.find_element(By.CLASS_NAME, 'ProductName').text
                date = driver.find_element(By.CLASS_NAME, 'date').text
                price = int(driver.find_element(By.CLASS_NAME, "cost").text.replace('원','').replace(',',''))
                img_url = driver.find_element(By.CLASS_NAME, 'image').get_attribute('src')
                # content에 대한 값을 가져오기 위해 componenets > componenet > p.text에 접근해야함
                components = driver.find_elements(By.CLASS_NAME, 'se-component-content')
                content = ''

                for component in components:
                    p_tags = component.find_elements(By.TAG_NAME, "p")
                    for p_tag in p_tags:
                        content += p_tag.text
                # url 하나에 대해서 sample 생성
                sample = {"title" : title, "date" :  date, "price" : price, "img_url" : img_url, "content" : content, "location" : None, 'status' : status, 'post_url' : url}

                sample = pd.DataFrame([sample])
                df = pd.concat([df, sample], ignore_index=True)
                error_count = 0
            except Exception as e:
                error_count += 1
                print("Error occur", e, ", Error_count:", error_count)
                time.sleep(4)
                continue
                
    
    df.to_csv(file_path + "_Scraping.csv", index=False, encoding='utf-8-sig')

if __name__ == "__main__" :
    file_path = "selling_URL/selling_url_naracafe1.txt"
    # status = soldout 게시글 추출이면 1, selling 게시글 추출이면 0
    status = 0
    Scrap_naracafe(file_path, status)


            



