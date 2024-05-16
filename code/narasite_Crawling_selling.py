import requests
import time
from bs4 import BeautifulSoup


def Get_soldout_post(item, min_price, max_price):
    root_url ="https://web.joongna.com"
    headers = {"User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"}
    
    for page_num in range(1,126):
        error_count = 0
        error_tries = 2
        try:
            print(page_num)
            path_url = f"/search/{item}?page={page_num}&sort=RECENT_SORT&minPrice={min_price}&maxPrice={max_price}&productFilterType=APP"
            data = requests.get(root_url + path_url, headers=headers).text
            soup = BeautifulSoup(data, 'html.parser')
            soup = soup.find('main').find('div', "w-full text").find('ul').find_next('ul').find_next('ul').find_all("li")

            with open(f"selling_url/selling_url_{min_price}-{max_price}.txt", 'a') as f:
                for post in soup:
                    selling_post_url = root_url + post.find('a')['href']
                    f.write(selling_post_url)
                    f.write('\n')
            error_count = 0
        except Exception as e:
            error_count += 1
            print(f"Error occur: {e}, Error Count: {error_count}")
            if error_count > error_tries:
                break
            time.sleep(5)
            continue

if __name__ == "__main__" :
    item = "%EC%95%84%EC%9D%B4%ED%8F%B014"
    min_price = 300000
    max_price = 1000000

    Get_soldout_post(item, min_price, max_price)
    