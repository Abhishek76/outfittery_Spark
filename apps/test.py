from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType
import pickle
#.config("spark.sql.defaultSizeInBytes", "100000")\
def init_spark():
  spark = SparkSession.builder\
    .appName("trip-app")\
    .config("spark.jars", "/opt/spark-apps/postgresql-42.2.22.jar")\
    .config("spark.sql.join.preferSortMergeJoin", "true")\
    .config("spark.sql.autoBroadcastJoinThreshold", '1')\
    .enableHiveSupport()\
    .getOrCreate()
  sc = spark.sparkContext
  return spark,sc

def main():
  url = "jdbc:postgresql://postgres:5432/german"
  

  file = "/opt/spark-data/GermanStackExchange"

  properties_file = open(file+"/properties.obj",'rb')

  properties = pickle.load(properties_file)
  properties_file.close()

  print(properties)



  
  spark,sc = init_spark()

  df_Users_temp = spark.read.parquet( "/opt/spark-data/test.parquet")
  df_Users_temp.show(5)
    


  
  df_Users_temp.write.jdbc(url=url, table="stack.user2", mode='append', properties=properties)

  df = sqlContext.read.format("jdbc").options(url=url, table="stack.user2", mode='append', properties=properties).load()
  df.show(5)
  # Filter invalid coordinates
  # df.where("latitude <= 90 AND latitude >= -90 AND longitude <= 180 AND longitude >= -180") \
  #   .where("latitude != 0.000000 OR longitude !=  0.000000 ") \
  #   .write \
  #   .jdbc(url=url, table="mta_reports", mode='append', properties=properties) \
  #   .save()
  
if __name__ == '__main__':
  main()