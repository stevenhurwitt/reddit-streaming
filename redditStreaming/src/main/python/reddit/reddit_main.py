from PyQt5.QtWidgets import QApplication
from PyQt5.QtWebEngineWidgets import QWebEngineView
import pandas as pd
from pyspark.sql import SparkSession

bucket = "reddit-streaming-stevenhurwitt-2"
subreddit = "technology"
read_path = f"s3a://{bucket}/{subreddit}_clean/"
# data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
spark = SparkSession.builder.appName("DataFrame to HTML").enableHiveSupport().getOrCreate()
df = spark.read.format("delta").load(read_path)
df.head()

pandas_df = df.toPandas()
# spark = SparkSession.builder.appName("DataFrame to HTML").enableHiveSupport().getOrCreate()

html_table = pandas_df.to_html()

app = QApplication([])
view = QWebEngineView()
view.setHtml(html_table)
view.show()
app.exec_()
