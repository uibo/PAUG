import requests
from bs4 import BeautifulSoup

url = "https://web.joongna.com/product/164119876"
data = requests.get(url).text
soup = BeautifulSoup(data, 'html.parser')
title = soup.find('div', "flex items-center justify-between mb-1").text
price = int(soup.find('div', "flex items-center mb-2 lg:mb-3").text.replace('원', '').replace(',',''))
date = soup.find('div', "flex items-center justify-between mb-4 text-xs font-normal").find_all('span')


print(date)
