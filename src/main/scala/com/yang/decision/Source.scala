package com.yang.decision

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

/**
  * 数据源抽象
  *
  * @author yangfan
  * @since 2021/3/18
  * @version 1.0.0
  */
abstract class Source(spark: SparkSession, processDay: String) {

  def loadData(): DataFrame
}

/**
  * 数据源
  *
  * @author yangfan
  * @since 2021/3/18
  * @version 1.0.0
  */
object Source {

  def loadData(sourceClass: String,
               spark: SparkSession,
               processDay: String,
               parallelism: Int): DataFrame = Class.forName(sourceClass)
    .getConstructor(classOf[SparkSession], classOf[String])
    .newInstance(spark, processDay)
    .asInstanceOf[Source]
    .loadData()
    .repartition(parallelism)
    .cache()

  /**
    * @version 2021/3/29 1.0.1 对数据源抽象部分进行了改动，
    *          由实现Source类变更为在Json配置文件中配置，原先的数据源ETL拿到工程外部去做。
    */
  def loadData(spark: SparkSession,
               conf: Configuration): DataFrame = {
    val schema = StructType(
      conf.conf("source.schema")
        .split(",")
        .map(item => StructField(item.trim, StringType))
    )
    val inputDF = if (conf.isLocal) {
      getInputDataLocal(spark)
    } else {
      getInputDataCluster(spark, conf)
    }
    val rdd = inputDF.rdd.map(row => {
      val id = row.getAs[String](0)
      val featuresStr = row.getAs[String](1)
      Row.fromSeq(id +: featuresStr.split("_", -1).map(emptyStr2Null))
    })
    val data = spark.createDataFrame(rdd, schema).repartition(conf.parallelism)
    if (conf.isCache) data.cache()
    data
  }

  private def emptyStr2Null(str: String): String = str match {
    case s@_ if null != s && !s.isEmpty => s
    case _ => null
  }

  private def getInputDataLocal(spark: SparkSession): DataFrame = {
    spark.read.format("csv")
      .option("header", "true")
      .option("multiLine", value = true)
      .option("quote", "\"")
      .option("escape", "\"")
      .load("C:\\Users\\Administrator\\Desktop\\项目文档\\决策树模型工程\\age.csv")
  }

  private def getInputDataCluster(spark: SparkSession,
                                  conf: Configuration): DataFrame = {
    val source = conf.conf("source").split(",").map(_.trim)
    val tableName = source(0)
    val model = source(1)
    spark.sql(
      s"""
         |select
         |  id
         |  ,features_str
         |from $tableName
         |where dt = '${conf.processTime}'
         |and model = '$model'
       """.stripMargin
    )
  }
}
