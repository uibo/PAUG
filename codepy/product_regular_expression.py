import re

iphone14_dict = {
    'iPhone14':re.compile(r'(?i)(?:iPhone|아이폰)\s?14(?!(?:\s?(?:Pro|Plus|Pro\s?Max|프로|플러스|프로\s?맥스)))'),
    'iPhone14Plus':re.compile(r'(?i)(?:iPhone|아이폰)\s?14\s?(?:Plus|플러스)'),
    'iPhone14Pro':re.compile(r'(?i)(?:iPhone|아이폰)\s?14\s?(?:Pro|프로)(?!(?:\s?(?:Max|맥스)))'),
    'iPhone14ProMax':re.compile(r'(?i)(?:iPhone|아이폰)\s?14\s?(?:Pro|프로)\s?(?:Max|맥스)')                   
}

iphone13_dict = {
    'iPhone13':re.compile(r'(?i)(?:iPhone|아이폰)\s?13(?!(?:\s?(?:Pro|Plus|Pro\s?Max|프로|플러스|프로\s?맥스)))'),
    'iPhone13Plus':re.compile(r'(?i)(?:iPhone|아이폰)\s?13\s?(?:Plus|플러스)'),
    'iPhone13Pro':re.compile(r'(?i)(?:iPhone|아이폰)\s?13\s?(?:Pro|프로)(?!(?:\s?(?:Max|맥스)))'),
    'iPhone13ProMax':re.compile(r'(?i)(?:iPhone|아이폰)\s?13\s?(?:Pro|프로)\s?(?:Max|맥스)')                   
}

product_list = {
    'iphone14':iphone14_dict,
    'iphone13':iphone13_dict
}


