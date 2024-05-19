import requests
import time
from bs4 import BeautifulSoup

def get_postUrls(item, status):
    headers = {"User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"}
    with open(f"{status}/{item[0]}_{status}_Url_site.txt", 'a') as f:
        # 가격대를 30만~60만, 60만~ 90만, ... 210만~240만까지 나눠서 검색 이래야 오래전에 올린 게시글도 검색됨
            for i in range(1,8):
                min_price = 300000 * (i)
                max_price = 300000 * (i + 1)
                for page_num in range(1,126):
                    try:
                        if status == 'soldout':
                            url = f"https://web.joongna.com/search/{item[1]}?page={page_num}&saleYn=SALE_Y&sort=RECENT_SORT&minPrice={min_price}&maxPrice={max_price}&productFilterType=APP"
                            res_body = requests.get(url, headers=headers).text
                            soup = BeautifulSoup(res_body, 'html.parser')
                            post_section = soup.find('main').find('div', "w-full text").find('ul').find_next('ul').find_next('ul').find_all("li")
            
                            for post in post_section:
                                if post.find("div", string = '판매완료'):
                                    post_url = url[:23] + post.find('a')['href']
                                    f.write(post_url)
                                    f.write('\n')
                        else:
                            url = f"https://web.joongna.com/search/{item[1]}?page={page_num}&sort=RECENT_SORT&minPrice={min_price}&maxPrice={max_price}&productFilterType=APP"
                            res_body = requests.get(url, headers=headers).text
                            soup = BeautifulSoup(res_body, 'html.parser')
                            post_section = soup.find('main').find('div', "w-full text").find('ul').find_next('ul').find_next('ul').find_all("li")

                            for post in post_section:
                                post_url = url[:23] + post.find('a')['href']
                                f.write(post_url)
                                f.write('\n')
                    except Exception as e:
                        print(f"{e}, Restart after 3 seconds")
                        page_num -=1
                        time.sleep(3)
                        continue
                    print('.', end='', flush=True)

if __name__ == "__main__" :
    status = ['selling', 'soldout']
    # 아래 변수들 조작 필요
    item = ["iphone14", "%EC%95%84%EC%9D%B4%ED%8F%B014"]
    status = [0]
    #
    get_postUrls(item, status)
    