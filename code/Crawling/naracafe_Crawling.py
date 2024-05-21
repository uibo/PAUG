import requests
from bs4 import BeautifulSoup
from datetime import datetime 
import time

"""year하고 month 입력시, (year, month, 1일)~ (year,month, 15일) and 
(year, month, 16일) ~ (year, month, 말일)까지 기간 셋팅, 
검색에 대해 최대 100페이지 확인가능한데 2주 넘는 기간은 100페이지 내에 표시가 안되서 기간을 나누기 위함"""
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
def get_postUrls (item, date, status) :
    headers = {"User-Agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"}
    with open(f"{status}/{item[0]}_{status}_Url_cafe.txt", 'a') as f:
        for page_num in range(1,101):
            #진행 중임을 확인하기 위한 터미널출력
            print('.',end='',flush=True)
            try:
                url = f"https://cafe.naver.com/ArticleSearchList.nhn?search.clubid=10050146&search.media=0&search.searchdate={date}&search.defaultValue=1&search.exact=&search.include=&userDisplay=50&search.exclude=%B8%C5%C0%D4+%B1%B3%C8%AF+%C4%C9%C0%CC%BD%BA+%BB%F0%B4%CF%B4%D9+%B1%B8%B8%C5&search.option=0&search.sortBy=date&search.searchBy=1&search.searchBlockYn=0&search.includeAll=&search.query={item[1]}&search.viewtype=title&search.page={page_num}"

                res_body = requests.get(url, headers=headers).text
                Soup = BeautifulSoup(res_body, 'html.parser')

                # 게시글들을 포함하고 있는 Tag 찾기
                content = Soup.find("div", id = "content-area").find("div", id = "main-area").find("div", "article-board m-tcol-c").find_next("div")

                # 그중 에서 판매된 게시글 찾기
                status_dict = {'soldout' : 'sellout', 'selling' : 'selling'}
                content = content.find_all('span', f"list-i-{status_dict[status]}")

                #현재 페이지에서 게시글이 탐색됐을 시
                if content:
                    for post in content:
                        post_url = url[:22] + post.find_previous_sibling()['href'][6:]
                        f.write(post_url)
                        f.write('\n')
            except Exception as e:
                print(f"{e}, restart after 2 second")
                time.sleep(2)
                page_num -= 1
                continue

#시작 기간부터 24년 5월까지 해당 품목에 대한 판매글 추출
def Crawling_until_today(year, month, item, status):
    while year != 2024 or month != 6:
        date = set_date(year, month)
        # 1일부터 15일
        get_postUrls(item, date[0], status)
        # 16일부터 말일
        get_postUrls(item, date[1], status)
        month += 1
        if month > 12:
            month -= 12
            year += 1

if __name__ == "__main__" :
    status_list = ['selling', 'soldout']
    # 매입 교환 케이스 삽니다 구매 포함된 게시글 필터링 적용됨 "%B8%C5%C0%D4+%B1%B3%C8%AF+%C4%C9%C0%CC%BD%BA+%BB%F0%B4%CF%B4%D9+%B1%B8%B8%C5"
    # 아래 변수들 조작 필요
    year = 2022
    month = 9
    item = ["iphone13", "%BE%C6%C0%CC%C6%F913"] 
    status = status_list[1]
    #
    Crawling_until_today(year, month, item, status)