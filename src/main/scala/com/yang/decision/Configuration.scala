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
  final val MODEL_CONF_FILE_PATH = "decision.tree.file.path"
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
  /**
    * 是否开启支持稀疏数组，默认false
    */
  final val ENABLE_SPARSE_VECTOR = "enable.sparse.vector"

  def init(map: Seq[(String, String)]): Configuration = {
    val conf = new Configuration
    conf.conf ++= loadDefaultConf
    conf.conf ++= map
    conf
  }

  /**
    * 加载默认配置
    */
  def loadDefaultConf: Seq[(String, String)] = {
    val df = new SimpleDateFormat("yyyyMMdd")

    def currentDate: String = df.format(System.currentTimeMillis())

    Seq((Configuration.PROCESS_TIME, currentDate),
      (Configuration.PARALLELISM, String.valueOf(300)),
      (Configuration.IS_LOCAL, String.valueOf(false)),
      (Configuration.IS_CACHE, String.valueOf(true)),
      (Configuration.ENABLE_SPARSE_VECTOR, String.valueOf(false))
    )
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

  def copy: Configuration = Configuration.init(conf.toSeq)

  def modelConfFilePath: String = conf(Configuration.MODEL_CONF_FILE_PATH)

  def processTime: String = conf(Configuration.PROCESS_TIME)

  def parallelism: Int = conf(Configuration.PARALLELISM).toInt

  def isLocal: Boolean = conf(Configuration.IS_LOCAL).toBoolean
}
