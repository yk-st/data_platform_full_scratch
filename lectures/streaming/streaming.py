#pysparkに必要なライブラリを読み込む
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession

#sudo apt install -y git vim

#spark sessionの作成
# spark.ui.enabled trueとするとSparkのGUI画面を確認することができます
# spark.eventLog.enabled true　とすると　GUIで実行ログを確認することができます
# GUIなどの確認は最後のセクションで説明を行います。
spark = SparkSession.builder \
    .appName("etl") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("spark.sql.session.timeZone", "JST") \
    .config("spark.ui.enabled","true") \
    .config("spark.eventLog.enabled","true") \
    .config("spark.jars.packages", "org.apache.spark:spark-streaming_2.13:3.2.2,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.2,org.apache.spark:spark-avro_2.12:3.2.2") \
    .enableHiveSupport() \
    .getOrCreate()

# パッケージを複数渡したい時は「,」で繋いで渡します。
# Sparkのバージョンにしっかりと合わせます(今回はSparkのバージョンが3.2.2を使っています。)。

# 処理を停止しないようにします
# ssc = StreamingContext(spark.sparkContext, 1)

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka_big:9092") \
  .option("subscribe", "pyspark-topic") \
  .load()

#　ちなみに別の方法も
# producer = KafkaProducer(bootstrap_servers="kafka_big:9092", value_serializer=lambda v: json.dumps(v).encode('utf-8'))
# kafkaStream = KafkaUtils.createDirectStream(
#     ssc, "pyspark-topic", {"metadata.broker.list": "kafka_big:9092"})

file_stream = df \
  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
  .writeStream \
  .format("parquet") \
  .option("path", "/tmp/share_file/datalake/web_actions/") \
  .outputMode("append") \
  .partitionBy("key") \
  .trigger(processingTime="5 seconds") \
  .option("checkpointLocation", "/tmp/kafka/parquet/") \
  .start()

# 処理が終了しないようにする
file_stream.awaitTermination()