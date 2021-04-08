package com.yang.decision

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
    * 本地模式下，数据源文件，无默认值
    */
  final val LOCAL_SOURCE = "local.source"
  /**
    * 是否启用cache，默认true
    */
  final val IS_CACHE = "is.cache"
  /**
    * 是否开启支持稀疏数组，默认true
    */
  final val ENABLE_SPARSE_VECTOR = "enable.sparse.vector"

  def init(map: Seq[(String, String)]): Configuration = {
    val conf = new Configuration
    conf.conf ++= map
    conf
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

  def modelConfFilePath: String = conf(Configuration.MODEL_CONF_FILE_PATH)

  def processTime: String = conf(Configuration.PROCESS_TIME)

  def parallelism: Int = conf(Configuration.PARALLELISM).toInt

  def isLocal: Boolean = conf(Configuration.IS_LOCAL).toBoolean
}
