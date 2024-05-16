from selenium import webdriver
from selenium.webdriver.common.by import By
import time
import requests
import pandas as pd

url = "https://cafe.naver.com/joonggonara?iframe_url_utf8=%2FArticleRead.nhn%253Fclubid%3D10050146%2526page%3D5%2526userDisplay%3D50%2526inCafeSearch%3Dtrue%2526searchBy%3D1%2526query%3D%25EC%2595%2584%25EC%259D%25B4%25ED%258F%25B014%2526includeAll%3D%2526exclude%3D%25EB%25A7%25A4%25EC%259E%2585%2520%25EA%25B5%2590%25ED%2599%2598%2520%25EC%25BC%2580%25EC%259D%25B4%25EC%258A%25A4%2520%25EC%2582%25BD%25EB%258B%2588%25EB%258B%25A4%2526include%3D%2526exact%3D%2526searchdate%3D2024-03-162024-03-31%2526media%3D0%2526sortBy%3Ddate%2526articleid%3D1047170429%2526referrerAllArticles%3Dtrue"




driver = webdriver.Chrome()
driver.get(url)
time.sleep(1)
iframe_element = driver.find_element(By.ID, "cafe_main") 
time.sleep(1)
driver.switch_to.frame(iframe_element)
time.sleep(1)
title = driver.find_element(By.CLASS_NAME, 'ProductName').text
date = driver.find_element(By.CLASS_NAME, 'date').text
price = driver.find_element(By.CLASS_NAME, "cost").text
img_url = driver.find_element(By.CLASS_NAME, 'image').get_attribute('src')
components = driver.find_elements(By.CLASS_NAME, 'se-component-content')
content = ''

for component in components:
    p_tags = component.find_elements(By.TAG_NAME, "p")
    for p_tag in p_tags:
        content += p_tag.text

sample = (title, date, price, img_url, content)

sample = pd.DataFrame(sample)
print(sample)



