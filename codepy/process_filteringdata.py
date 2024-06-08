from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, to_date, col, array, when
from pyspark.sql.types import IntegerType, ArrayType, StringType
import sys
import re
from product_regular_expression import product_list


# model 분류를 위한 spark.udf작성
def classify_model(title):
    for model in model_dict:
        match = model_dict[model].search(title)
        if match:
            return model
    return 'None'
classify_model_udf = udf(classify_model, StringType())

# storage 분류를 위한 spark.udf작성
def classify_storage_feature(feature_list, title, context):
    pattern = re.compile(r'(?<!\d)(64|128|256|512|1)(?!\d)', re.IGNORECASE)
    
    title = title if title is not None else ""
    context = context if context is not None else ""

    # title에서 storage 추출
    match = pattern.search(title)
    if match:
        if match.group(1) == '1':
            feature_list.append("1024GB")
        else:
            feature_list.append(match.group(1)+"GB")
        return feature_list

    # title에서 추출 못할 시 context에서 storage 추출
    match = pattern.search(context)
    if match:
        if match.group(1) == '1':
            feature_list.append("1024GB")
        else:
            feature_list.append(match.group(1)+"GB")
        return feature_list
    return feature_list
classify_storage_feature_udf = udf(classify_storage_feature, ArrayType(StringType()))

# 제품의 일반적인 특징을 뽑아내기 위한 spark.udf작성
def extract_general_feature(feature_list, context):
    # context 없는 경우 feature 추출하지 않고 바로 return
    if context is None:
        return feature_list
    
    # 제품이 갖고있는 특징들이 feature_list에 삽입되고 return됨
    pattern = re.compile(r'.*미개봉.*')
    if pattern.search(context):
        feature_list.append('미개봉')
        return feature_list

    # 일반적인 특징을 뽑아내기 위해 정규식 구성에 사용될 단어 및 형태소
    checking_feature_list = ['기스', '깨짐', '잔상', '흠집', '파손', '찍힘']
    checking_morpheme_list = ['있', '존재', '정도']

    for checking_feature in checking_feature_list:
        for checking_morpheme in checking_morpheme_list:
            pattern = re.compile(fr".*{checking_feature}.{{0,10}}{checking_morpheme}.*")
            match = pattern.search(context)
            
            if match:
                feature_list.append(checking_feature)
                break
                    
    checking_feature_list2 = ['부품용']  
    for checking_feature in checking_feature_list2:
        if checking_feature in context:
            feature_list.append(checking_feature)
                
    return feature_list
extract_general_feature_udf = udf(extract_general_feature, ArrayType(StringType()))

# 애플 제품 한정, 애플케어플러스를 추출하기 위한 spark.udf작성
def extract_applecare_feature(feature_list, context):
    # context 없는 경우 feature 추출하지 않고 바로 return
    if context is None:
        return feature_list
        
    pattern = re.compile(r"(케어|캐어|애케|애캐|애플케어|애플캐어|애케플|애캐플).{0,15}(포함|까지|있|적용)")
    match = pattern.search(context)
    if match:
        feature_list.append('애플케어플러스')
        return feature_list
    return feature_list
extract_applecare_feature_udf = udf(extract_applecare_feature, ArrayType(StringType()))

def extract_battery(context):
    # battery 값 확인할 수 없다는 의미로 -1
    if context is None:
        return -1
        
    pattern = re.compile(r'.*미개봉.*')
    if pattern.search(context):
        return 100
        
    pattern = re.compile(r".*배터리.{0,7}(\d{2,3}).{0,3}\s*(퍼센트|프로|%|퍼)")
    match = pattern.search(context)
    if match:
        efficiency = match.group(1)
        
        if efficiency == '00':
            efficiency = '100'
        return int(efficiency)
        
    pattern = re.compile(r".*배터리.{0,7}(\d{2,3}).*")
    match = pattern.search(context)
    if match:
        efficiency = match.group(1)
        
        if efficiency == '00':
            efficiency = '100'
        return int(efficiency)
    return -1
extract_battery_udf = udf(extract_battery, IntegerType())

def feature_list_to_string(feature_list):
    return str(feature_list)
feature_list_to_string_udf = udf(feature_list_to_string, StringType())


def process_filteringdata(filteringdata_file_path, file_name,product_name):
    spark = SparkSession.builder.appName("processing").getOrCreate()

    # 제품명_filtering.csv 읽어오기
    df = spark.read.csv(filteringdata_file_path+file_name, header = True, multiLine=True, quote='"', escape='"')
    df = df.withColumn("feature_list", array())

    global model_dict 
    model_dict = product_list[product_name]

    df = df.withColumn('model', classify_model_udf(df.title))
    df = df.withColumn('feature_list', classify_storage_feature_udf(df.feature_list, df.title, df.context))
    df = df.withColumn('feature_list', extract_general_feature_udf(df.feature_list, df.context))
    df = df.withColumn('feature_list', extract_applecare_feature_udf(df.feature_list, df.context))
    df = df.withColumn('battery', extract_battery_udf(df.context))

    # 모델 분류 안되는 행 삭제
    df = df.filter(col("model") != "None")
    # location 채워 넣기
    df = df.withColumn("location", when((df.location.isNull()) | (df.location == ""), "-").otherwise(df.location))
    # feature_list를 문자열로 변환
    df = df.withColumn('feature_list', feature_list_to_string_udf(df.feature_list))

    # cleaning data csv 생성
    output_file_path = filteringdata_file_path +product_name+'_cleaning.csv'
    df = df.select(['model', 'feature_list', 'battery', 'upload_date', 'price', 'status', 'location', 'imgUrl', 'url','context'])#.toPandas()
    # df.to_csv(output_file_path, header=True, index = False, encoding='utf-8-sig')
    df.write.csv(output_file_path, header=True, quote='"', escape='"')
    print("Created,", output_file_path)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("usage: python3 ~.py <filteringdata_file_path> <file_name> <product_name>" )
        print("example: python3 process_filteringdata.py /home/ubuntu/Desktop/data/ iphone14_filtering.csv iphone14")
        sys.exit(1)

    filteringdata_file_path = sys.argv[1]
    file_name = sys.argv[2]
    product_name = sys.argv[3]

    process_filteringdata(filteringdata_file_path, file_name ,product_name)

