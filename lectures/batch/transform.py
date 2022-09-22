#!/usr/bin/env python
# coding: utf-8
import sys
import argparse
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import json


def main():

    # spark sessionの作成
    spark = SparkSession.builder \
    .appName("etl") \
    .config("hive.exec.dynamic.partition", "true") \
    .config("hive.exec.dynamic.partition.mode", "nonstrict") \
    .config("spark.sql.session.timeZone", "JST") \
    .config("spark.ui.enabled","true") \
    .config("spark.eventLog.enabled","false") \
    .enableHiveSupport() \
    .getOrCreate()

    # jinko.csvの読み込み
    df=spark.read.parquet("/tmp/share_file/datalake/orders/")
    # 処理なし
    df.coalesce(1).write.mode('overwrite').csv("/tmp/share_file/datamart/orders/")

    # 最後は停止処理をします
    spark.stop()

if __name__ == '__main__':
    main()
