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
  spark,sc = init_spark()

  Users = file + "/Users.xml"
  df_Users = spark.read.format("com.databricks.spark.xml")         .option('rowTag', 'row').load(Users).select(
      F.col('_Id').cast('integer').alias("Id"),
      F.col('_Reputation').cast('integer').alias("Reputation"),
      F.col('_CreationDate').cast('timestamp').alias("CreationDate"),
      F.col('_DisplayName').alias("DisplayName"),
      F.col('_WebsiteUrl').alias("WebsiteUrl"),
      F.col('_LastAccessDate').cast('timestamp').alias("LastAccessDate"),
      F.col('_Location').alias("_Location"),  
      F.col('_Views').cast('integer').alias("Views"),
      F.col('_UpVotes').cast('integer').alias("UpVotes"),
      F.col('_DownVotes').cast('integer').alias("DownVotes"),
      F.col('_ProfileImageUrl').alias("ProfileImageUrl"),
      F.col('_AccountId').cast('integer').alias("AccountId")).dropDuplicates()
  df_Users.show(10)
  df_Users.write.parquet(file + "/Users.parquet")
  df_Users=spark.read.parquet(file + "/Users.parquet")


  # In[202]:


  df_Users.printSchema()


  # In[203]:


  badges = file + "/Badges.xml"
  df_Badges = spark.read.format("com.databricks.spark.xml")         .option('rowTag', 'row')         .load(badges).select(F.col('_Id').cast('integer'),
                              F.col('_Name'),
                              F.col('_UserId').cast('integer'),).dropDuplicates()
  df_Badges.write.parquet(file + "/Badges.parquet")
  df_Badges=spark.read.parquet(file + "/Badges.parquet")


  # In[204]:


  posts = file + "/Posts.xml"
  df_Posts = spark.read.format("com.databricks.spark.xml")         .option('rowTag', 'row')         .load(posts).select(F.col('_Id').cast('integer'),
                              F.col('_CreationDate').cast('timestamp'),
                              F.col('_OwnerUserId').cast('integer'),
                              F.col('_CommentCount').cast('integer'),).dropDuplicates()
  df_Posts.write.parquet(file + "/Posts.parquet")
  df_Posts=spark.read.parquet(file + "/Posts.parquet")


  # In[205]:


  comments = file + "/Comments.xml"
  df_Comments = spark.read.format("com.databricks.spark.xml")         .option('rowTag', 'row')         .load(comments).select(F.col('_Id').cast('integer'),
                              F.col('_CreationDate').cast('timestamp'),
                              F.col('_UserId').cast('integer'),).dropDuplicates()
  df_Comments.write.parquet(file + "/Comments.parquet")
  df_Comments=spark.read.parquet(file + "/Comments.parquet")





  df_Badges = df_Badges.withColumn('is_Editor', F.when(F.col('_Name') == "Editor", 1).otherwise(0))
  df_Badges = df_Badges.withColumn('is_critic', F.when(F.col('_Name') == "Critic", 1).otherwise(0)).dropDuplicates(['_UserId'])
  df_Badges = df_Badges.drop("_Id","_Name")



  # In[210]:


  df_commentAverageOverPeriod = df_Comments.groupBy("_UserId").agg(F.count("_UserId").alias("count"), F.max("_CreationDate").alias("max_date"), F.min("_CreationDate").alias("min_date")).orderBy(F.col("count").desc())
  df_commentAverageOverPeriod = df_commentAverageOverPeriod.withColumn("total_active_period", F.months_between(F.col("max_date"),F.col("min_date")))
  df_commentAverageOverPeriod = df_commentAverageOverPeriod.withColumn("total_active_period", F.floor(F.col("total_active_period")).cast(IntegerType())).withColumn("AverageComment_OverPeriod",  F.round((F.col("count") /( F.col("total_active_period")+1)),2))
  df_commentAverageOverPeriod = df_commentAverageOverPeriod.dropDuplicates(['_UserId'])

  df_commentAverageOverPeriod = df_commentAverageOverPeriod.drop("count","max_date","min_date","total_active_period")



  # In[211]:


  df_commentAverageActive = df_Comments.withColumn("yyyymm", F.date_format(F.col("_CreationDate"), "yyyy MM")).groupBy("_UserId","yyyymm").count()

  df_commentAverageActive = df_commentAverageActive.groupBy("_UserId").agg(F.count("_UserId").alias("Months_active"), F.sum("count").alias("total_comments"),F.max("yyyymm").alias("max_date"), F.min("yyyymm").alias("min_date"))

  df_commentAverageActive = df_commentAverageActive.withColumn("AverageValue_OverActiveMonths",  F.round((F.col("total_comments") / F.col("Months_active"))).cast(IntegerType()))
  df_commentAverageActive = df_commentAverageActive.dropDuplicates(['_UserId'])


  df_commentAverageActive = df_commentAverageActive.drop("total_comments","max_date","min_date","Months_active")



  # In[212]:


  df_Count_posts = df_Posts.groupBy("_OwnerUserId").agg(F.count("_OwnerUserId").alias("TotalPostsCount"))
  df_Count_posts = df_Count_posts.dropDuplicates(['_OwnerUserId'])
 


  # In[213]:


  df_last90days_posts = df_Posts.filter(F.col('_CreationDate') >= F.date_sub(F.current_date(), 90))
  df_last90days_posts = df_last90days_posts.groupBy("_OwnerUserId").agg(F.count("_OwnerUserId").alias("PostsCountLast90Days"))
  df_last90days_posts = df_last90days_posts.dropDuplicates(['_OwnerUserId'])
  


  # In[214]:


  df_lastcreation_posts = df_Posts.filter(F.col('_CommentCount') >= 1).groupBy("_OwnerUserId").agg(F.max("_CreationDate").alias("Last_creationDate"))
  df_lastcreation_posts = df_lastcreation_posts.dropDuplicates(['_OwnerUserId'])
  


  # In[215]:


  print(df_Users.count())


  # In[216]:


  df_Users_temp = df_Users.join(df_lastcreation_posts, df_Users.Id == df_lastcreation_posts._OwnerUserId,how='left')
  df_Users_temp = df_Users_temp.drop("_OwnerUserId","_UserId")
  print(df_Users_temp.count())


  # In[217]:



  df_Users_temp = df_Users_temp.join(df_Count_posts, df_Users_temp.Id == df_Count_posts._OwnerUserId,how='left')
  df_Users_temp = df_Users_temp.drop("_OwnerUserId")
  print(df_Users_temp.count())


  # In[218]:



  df_Users_temp = df_Users_temp.join(df_last90days_posts, df_Users_temp.Id == df_last90days_posts._OwnerUserId,how='left')
  df_Users_temp = df_Users_temp.drop("_OwnerUserId")
  print(df_Users_temp.count())


  # In[219]:





  # In[220]:



  df_Users_temp = df_Users_temp.join(df_Badges, df_Users_temp.Id == df_Badges._UserId,how='left')
  df_Users_temp = df_Users_temp.drop("_UserId")
  print(df_Users_temp.count())





  df_Users_temp = df_Users_temp.join(df_commentAverageOverPeriod, df_Users_temp.Id == df_commentAverageOverPeriod._UserId,how='left')
  df_Users_temp = df_Users_temp.drop("_UserId")
  df_Users_temp.count()





  df_Users_temp = df_Users_temp.join(df_commentAverageActive, df_Users_temp.Id == df_commentAverageActive._UserId,how='left')
  df_Users_temp = df_Users_temp.drop("_UserId")
  df_Users_temp.count()





  df_Users_temp.select(
      F.col('Id').cast('integer'),
      F.col('Reputation').cast('integer'),
      F.col('CreationDate').cast('timestamp'),
      F.col('DisplayName'),
      F.col('WebsiteUrl').cast('integer'),
      F.col('LastAccessDate').cast('timestamp'),
      F.col('_Location'),
      F.col('Views').cast('integer'),
      F.col('UpVotes').cast('integer'),
      F.col('DownVotes').cast('integer'),
      F.col('ProfileImageUrl'),
      F.col('AccountId').cast('integer'),
      F.col('Last_creationDate').cast('timestamp'),
      F.col('TotalPostsCount').cast('integer'),
      F.col('PostsCountLast90Days').cast('integer'),
      F.col('is_Editor').cast('integer'),
      F.col('is_critic').cast('integer'),
      F.col('AverageComment_OverPeriod').cast('float') ,
      F.col('AverageValue_OverActiveMonths').cast('float') 
      
  ).write.csv(file + '/UserOutputcsv.csv')
  df_Users_temp.write.jdbc(url=url, table="stack.user", mode='append', properties=properties)

  
if __name__ == '__main__':
  main()
  
