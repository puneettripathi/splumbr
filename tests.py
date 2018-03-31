from pyspark.sql import SparkSession
import io
import zipfile
import os
os.environ['JAVA_HOME'] = 'C:\Program Files\Java\jdk1.8.0_161'
spark = SparkSession \
            .builder \
            .appName("SparkETL") \
            .getOrCreate()
sc = spark.sparkContext
print(sc)
rdd = sc.binaryFiles("D:\\pgdds\\kaggle\\titanic\\test.zip")
def zip_extract(x):
    in_memory_data = io.BytesIO(x[1])
    file_obj = zipfile.ZipFile(in_memory_data, "r")
    files = [i for i in file_obj.namelist()]
    return [(file,file_obj.open(file).read().decode()) for file in files]


data_rdd = rdd.flatMap(lambda x : zip_extract(x)).flatMap(lambda x: x.split("\n")).map(lambda x: x.split(',')).filter(lambda x: x[0] not in ('PassengerId',''))

print(data_rdd.collect())
#print(sc.binaryFiles("D:\\pgdds\\kaggle\\titanic\\test.zip").flatMap(lambda x : zip_extract(x)).map(lambda x : x[1]).flatMap(lambda s: s.split("\n")).map(lambda x: x.split(',')).take(2))
#.map(lambda x : x[1]).flatMap(lambda s: s.split("\n"))

# My_RDD = rdd.map(lambda kv: (kv[0], Zip_open(kv[1])))
# My_RDD.collect()
#spark.read.csv("D:\\pgdds\\kaggle\\titanic\\test.csv").show()