from PyQt5.QtWidgets import QApplication
from PyQt5.QtWebEngineWidgets import QWebEngineView
import pandas as pd
from pyspark.sql import SparkSession

logger = logging.getLogger("redditStreaming")
bucket = "reddit-streaming-stevenhurwitt-2"
subreddit = "technology"
read_path = f"s3a://{bucket}/{subreddit}_clean/"
# data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
spark = SparkSession.builder.appName("DataFrame to HTML").enableHiveSupport().getOrCreate()
df = spark.read.format("delta").load(read_path)
sc = spark.SparkContext
sc.setLogLevel("INFO")

query = "select * from 'tweets' where user = 'ihearttyou2' and created_at > '2011-01-01T00:00:00Z' order by created_at desc;"
spark_df = df.select(query)
spark_df.head()

logger.info(df.shape)

pandas_df = df.toPandas()
logger.info("transformed df to pandas.")
# spark = SparkSession.builder.appName("DataFrame to HTML").enableHiveSupport().getOrCreate()

html_table = pandas_df.to_html()
logger.info("transformed pandas to html.")

app = QApplication([])
view = QWebEngineView()
view.setHtml(html_table)
view.show()
app.exec_()
