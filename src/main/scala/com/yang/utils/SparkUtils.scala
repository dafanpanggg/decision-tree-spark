package com.yang.utils

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * explain
  *
  * @author yangfan
  * @since 2021/3/2
  * @version 1.0.0
  */
object SparkUtils {

  def initSession(isLocal: Boolean,
                  appName: String,
                  conf: SparkConf = new SparkConf): SparkSession = {
    if (null == appName)
      throw new NullPointerException(
        "param exception, `appName` mast not be null!")
    if (isLocal) {
      SparkSession.builder()
        .appName(appName)
        .config(conf)
        .master("local[*]")
        .getOrCreate()
    } else {
      SparkSession.builder()
        .appName(appName)
        .config(conf)
        .enableHiveSupport()
        .getOrCreate()
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkUtils.initSession(isLocal = false, this.getClass.getSimpleName)
    println(spark)
  }
}
