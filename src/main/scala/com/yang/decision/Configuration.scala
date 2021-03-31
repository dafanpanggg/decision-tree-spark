package com.yang.decision

import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession

import scala.collection.mutable

/**
  * 配置属性
  *
  * @author yangfan
  * @since 2021/3/8
  * @version 1.0.0
  */
object Configuration {
  /**
    * 模型配置文件路径
    */
  final val MODEL_CONF_FILE_PATH = "model.conf.file.path"
  /**
    * 执行时间，默认当前时间
    */
  final val PROCESS_TIME = "process.time"
  /**
    * 并行度，默认300
    */
  final val PARALLELISM = "parallelism"
  /**
    * 是否本地模式，默认false
    */
  final val IS_LOCAL = "is.local"
  /**
    * 是否启用cache，默认true
    */
  final val IS_CACHE = "is.cache"

  private final val df1 = new SimpleDateFormat("yyyyMMdd")

  private def currentDate: String = df1.format(System.currentTimeMillis())

  def init(modelConfFilePath: String,
           processTime: String,
           parallelism: Int): Configuration = {
    val prop = new Configuration()
    val seq = Seq(
      (MODEL_CONF_FILE_PATH, modelConfFilePath),
      (PROCESS_TIME, processTime),
      (PARALLELISM, String.valueOf(parallelism)),
      (IS_LOCAL, String.valueOf(false)),
      (IS_CACHE, String.valueOf(true))
    )
    prop.conf ++= seq
    prop
  }

  def init(modelFilePath: String, processTime: String): Configuration = {
    init(modelFilePath, processTime, 300)
  }

  def init(modelFilePath: String): Configuration = {
    init(modelFilePath, currentDate)
  }

  def newInstance[T](className: String,
                     spark: SparkSession,
                     properties: Configuration): T = this.getClass.getClassLoader
    .loadClass(className)
    .getConstructor(classOf[SparkSession], classOf[Configuration])
    .newInstance(spark, properties)
    .asInstanceOf[T]
}

sealed class Configuration extends Serializable {

  val conf: mutable.Map[String, String] = mutable.Map[String, String]()

  def processTime: String = conf(Configuration.PROCESS_TIME)

  def modelConfFilePath: String = conf(Configuration.MODEL_CONF_FILE_PATH)

  def parallelism: Int = conf(Configuration.PARALLELISM).toInt

  def isLocal: Boolean = conf(Configuration.IS_LOCAL).toBoolean

  def isCache: Boolean = conf(Configuration.IS_CACHE).toBoolean
}
