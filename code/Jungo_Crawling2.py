import requests
from bs4 import BeautifulSoup
from datetime import datetime 
import time

#year하고 month 입력시 해당 년, 월 1일부터 15일까지 기간, 16일부터 말일까지 기간 셋팅
def set_date(year, month):
    month_days = {
        1: 31, 2: 28, 3: 31, 4: 30,
        5: 31, 6: 30, 7: 31, 8: 31,
        9: 30, 10: 31, 11: 30, 12: 31
    }
    start_date1 = str(datetime(year, month, 1).strftime('%Y-%m-%d'))
    end_date1 = str(datetime(year, month, 15).strftime('%Y-%m-%d'))

    start_date2 = str(datetime(year, month, 16).strftime('%Y-%m-%d'))
    end_date2 = str(datetime(year, month, month_days[month]).strftime('%Y-%m-%d'))

    return start_date1+end_date1, start_date2+end_date2

#해당 기간에 해당 품목에 판매글 추출
def Get_soldoutpost (item, date) :

    headers = {"User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"}

    for page_num in range(1,100):
        root_url = "https://cafe.naver.com"
        path_url = f"/ArticleSearchList.nhn?search.clubid=10050146&search.media=0&search.searchdate={date}&search.defaultValue=1&search.exact=&search.include=&userDisplay=50&search.exclude=%B8%C5%C0%D4+%B1%B3%C8%AF+%C4%C9%C0%CC%BD%BA+%BB%F0%B4%CF%B4%D9&search.option=0&search.sortBy=date&search.searchBy=1&search.searchBlockYn=0&search.includeAll=&search.query={item}&search.viewtype=title&search.page={page_num}"

        url = root_url + path_url
        data = requests.get(url,headers=headers).text
        Soup = BeautifulSoup(data, 'html.parser')

        # 게시글 section element 찾기
        content = Soup.find("div", id = "content-area").find("div", id = "main-area").find("div", "article-board m-tcol-c").find_next("div")

        # 판매된 게시글 찾기
        content = content.find_all('span', 'list-i-sellout')

        #현재 페이지에서 판매된 게시글이 탐색됐을 시
        if content:
            with open(f"../soldout_URL/soldout_url_{date}.txt", 'w') as f:
                for post in content:
                    post_url = root_url + post.find_previous_siblings()[0]['href'][6:]
                    f.write(post_url)
                    f.write('\n')

#시작 기간부터 24년 5월까지 해당 품목에 대한 판매글 추출
def Crawling_until_today(year, month, item):
    max_retries = 5
    retries = 0

    while year != 2024 or month != 6:
        date = set_date(year, month)
        try:
            # 1일부터 15일
            print(date[0])
            Get_soldoutpost(item, date[0])
            # 16일부터 말일
            print(date[1])
            Get_soldoutpost(item, date[1])
            retries = 0  # 성공 시 재시도 횟수 초기화
        except Exception as e:
            retries += 1
            print(f"Error occurred: {e}", f", Error Count: {retries}")
            if retries > max_retries:
                break
            time.sleep(5)  # 재시도 전 대기 시간
            continue
        month += 1
        if month > 12:
            month -= 12
            year += 1

if __name__ == "__main__" :

    # 매입 교환 케이스 %B8%C5%C0%D4+%B1%B3%C8%AF+%C4%C9%C0%CC%BD%BA+%BB%F0%B4%CF%B4%D9
    year = 2023
    month = 10
    item = "%BE%C6%C0%CC%C6%F914" # iphone 14
    
    Crawling_until_today(year, month, item)
        