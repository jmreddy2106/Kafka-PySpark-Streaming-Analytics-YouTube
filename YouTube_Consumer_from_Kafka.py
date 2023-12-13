import findspark
findspark.init(spark_home = 'C:/spark/spark-3.2.1-bin-hadoop2.7')
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
from pyspark.conf import SparkConf
from pyspark.sql.types import *
from cassandra.cluster import Cluster
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, FloatType, TimestampType, BooleanType, ArrayType
import matplotlib.pyplot as plt


## Connect to the cluster and keyspace
# cluster = Cluster(['127.0.0.1'], port=9042)
# session = cluster.connect()
# session.set_keyspace('youtube')
# session.execute("USE youtube")
topic_name_read = "youtube-search-data*"
kafkaServer = "localhost:9092"

# Enabling Spark Configuration and SparkSession
sconf = SparkConf().setMaster("local").setAppName("YouTubeReadKafKa")
sc = SparkContext(conf=sconf).setLogLevel('ERROR')
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
spark.conf.set("spark.sql.adaptive.enabled","true")

# Creating a JSON schema for the data to be read from Kafka
mySchema = StructType().add("kind", StringType())\
                       .add("etag", StringType())\
                       .add("items", ArrayType(StructType()\
                                                .add("kind", StringType())\
                                                .add("etag", StringType())\
                                                .add("id", StringType())\
												.add("snippet", StructType()\
													.add("publishedAt", TimestampType())\
													.add("channelId", StringType())\
                                                    .add("title", StringType())\
													.add("description", StringType())\
                                                    .add("thumbnails", StructType()\
                                                            .add("default", StructType().add("url", StringType())\
                                                                .add(".width", IntegerType())\
                                                                .add(".height", IntegerType()))\
                                                            .add("medium", StructType().add("url", StringType())\
                                                                .add(".width", IntegerType())\
                                                                .add(".height", IntegerType()))\
                                                            .add("high", StructType().add("url", StringType())\
                                                                .add(".width", IntegerType())\
                                                                .add(".height", IntegerType()))\
                                                            .add("standard", StructType().add("url", StringType())\
                                                                .add(".width", IntegerType())\
                                                                .add(".height", IntegerType()))\
                                                            .add("maxres", StructType().add("url", StringType())\
                                                                .add(".width", IntegerType())\
                                                                .add(".height", IntegerType())))\
                                                    .add("channelTitle", StringType())\
													.add("categoryId", StringType())\
                                                    .add("liveBroadcastContent", StringType())\
                                                    .add("localized", StructType()\
																.add("title", StringType())\
															    .add("description", StringType()))\
 
                                                    )\
                                            .add("contentDetails", StructType()\
                                                    .add("duration", StringType())\
                                                    .add("dimension", StringType())\
                                                    .add("definition", StringType())\
                                                    .add("caption", StringType())\
                                                    .add("licensedContent", BooleanType())\
                                                    .add("contentRating", StructType())\
                                                    .add("projection", StringType())\
                                                )\
                                            .add("statistics", StructType()\
                                                .add("viewCount", StringType())\
                                                .add("likeCount", StringType())\
                                                .add("favoriteCount", StringType())\
                                                .add("commentCount", StringType()))\
                                                        
                                                )\
                        )\
                        .add("nextPageToken", StringType())\
                        .add("pageInfo", StructType()\
                        .add("totalResults", IntegerType())\
                        .add("resultsPerPage", IntegerType()))

df = spark.read.format("kafka")\
    .option("kafka.bootstrap.servers", kafkaServer)\
    .option("subscribePattern", topic_name_read)\
    .option("startingOffsets", "earliest")\
    .load()\
    .selectExpr("CAST(value AS STRING)")\
    .select(F.from_json("value", mySchema).alias("data"))\
    .select("data.*")
    

query = df.select("items.snippet.categoryId")

# Pyspark dataframe to python dictionary
cat_dict={}
for i in query.collect():
    for k in i.asDict()['categoryId']:
        if k not in cat_dict:
            cat_dict[k] = 1
        else:
            cat_dict[k] += 1


categ_dict={'1': 'Film & Animation', '2': 'Autos & Vehicles', '10': 'Music', '15': 'Pets & Animals', '17': 'Sports', '18': 'Short Movies', '19': 'Travel & Events', '20': 'Gaming', '21': 'Videoblogging', '22': 'People & Blogs', '23': 'Comedy', '24': 'Entertainment', '25': 'News & Politics', '26': 'Howto & Style', '27': 'Education', '28': 'Science & Technology', '29': 'Nonprofits & Activism', '30': 'Movies', '31': 'Anime/Animation', '32': 'Action/Adventure', '33': 'Classics', '34': 'Comedy', '35': 'Documentary', '36': 'Drama', '37': 'Family', '38': 'Foreign', '39': 'Horror', '40': 'Sci-Fi/Fantasy', '41': 'Thriller', '42': 'Shorts', '43': 'Shows', '44': 'Trailers'}

new_dict = {}
for key,item in cat_dict.items():
    new_dict[categ_dict[key]] = item
print(new_dict)

#plot the data
x = list(new_dict.keys())
y = list(new_dict.values())
plt.bar(x,y)
plt.tight_layout()
plt.xticks(rotation = 90)
plt.show()