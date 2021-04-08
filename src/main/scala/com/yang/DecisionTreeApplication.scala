package com.yang

import java.text.SimpleDateFormat

import com.yang.decision.build._
import com.yang.decision.{Configuration, Source}
import com.yang.utils.{FileUtils, SparkUtils}
import org.slf4j.LoggerFactory

/**
  * 决策树模型抽象工程Application入口类
  * 1、解析入参args，获取执行日期（processDay）、
  * 配置文件路径（configFilePath）、并行度（parallelism）
  * 2、从HDFS读取配置文件，解析字段（schema）、
  * 数据源Class（source）、输出表（output）、规则集合（rules）
  * 3、将rules并解析为一棵二叉树，同时绑定树节点规则
  * 4、遍历解析后的二叉树（parsedTree），根据节点规则绑定各节点的数据集（dataFrame），
  * 生成绑定数据后的二叉树（boundTree）
  * 5、遍历全部叶子节点（Calculation），叶子节点的数据即为不同规则下的结果集，
  * 合并（union）全部叶子节点数据后，即为最终结果集
  * 执行时会输出parsed tree、bound tree以及physical plan
  * 使用者需要提供两点：
  * 1、【配置文件】，格式参照：AgeGroupRule.json
  * 2、【自定义数据源】，需要实现[[com.yang.decision.Source]]抽象类
  * 如果有新的模型需求，需要实现[[com.yang.decision.model.Model]]抽象类，来扩展决策树的模型类别
  *
  * @author yangfan
  * @since 2021/3/18
  * @version 2021/3/18 1.0.0
  * @version 2021/3/29 1.0.1 对数据源抽象部分进行了改动，由实现Source类变更为在Json配置文件中配置
  * @version 2021/3/29 1.0.2 XGBoot模型在计算时，需要传入一个特征向量，需要使用Array<Double>类型。
  *          经过测试，发现这种复杂类型在数据传输过程中需要占用较大的内存，影响计算效率。
  *          解决方案：在向量维度较多时，我们提供将向量转换成稀疏数组Array<Array<Double>>，
  *          再将其转为字符串格式，这样在数据传递过程中就能大幅减少内存占用。
  * @version 2021/3/29 1.0.3 增加了节点对PMML的支持，
  *          增加Model实现类[[com.yang.decision.model.PMMLModel]]
  */
object DecisionTreeApplication {

  private final val logger = LoggerFactory.getLogger(this.getClass)

  private def readInputArgs(args: Array[String]): Seq[(String, String)] = {
    if (null == args || args.length < 1) {
      throw new IllegalArgumentException(
        s"`readInputArgs` method Exception, " +
          s"args length at least 1 but get `${args.mkString(",")}` !")
    }
    logger.error("\n== input args ==\n{}", args.mkString(","))
    try {
      args.length match {
        case 1 => Seq((Configuration.MODEL_CONF_FILE_PATH, args(0)))
        case 2 => Seq(
          (Configuration.MODEL_CONF_FILE_PATH, args(0)),
          (Configuration.PROCESS_TIME, args(1))
        )
        case _ => Seq(
          (Configuration.MODEL_CONF_FILE_PATH, args(0)),
          (Configuration.PROCESS_TIME, args(1)),
          (Configuration.PARALLELISM, args(2))
        )
      }
    } catch {
      case _: Exception =>
        throw new IllegalArgumentException(
          s"`readInputArgs` method Exception, args type change failed !")
    }
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
      (Configuration.ENABLE_SPARSE_VECTOR, String.valueOf(true))
    )
  }

  def main(args: Array[String]): Unit = {

    val conf = Configuration.init(loadDefaultConf)

    /**
      * args0：模型配置文件路径，必传
      * args1：执行日期，不传则默认当前日期
      * args2：并行度，不传则默认300
      */
    conf.conf ++= readInputArgs(args)
    /*conf.conf(Configuration.PARALLELISM) = String.valueOf(1)
    conf.conf(Configuration.IS_LOCAL) = String.valueOf(true)
    conf.conf(Configuration.LOCAL_SOURCE) = "pmml_test.csv"
    conf.conf(Configuration.IS_CACHE) = String.valueOf(false)*/

    val modelConfigText = if (conf.isLocal) {
      FileUtils.readFromLocal(conf.modelConfFilePath)
    } else {
      FileUtils.readFromHDFS(conf.modelConfFilePath)
    }
    logger.error("\n== model config text ==\n{}", modelConfigText)

    /**
      * 解析Json为一棵二叉树，同时绑定树节点规则
      */
    val parsedTree = Parser.parse(modelConfigText, conf)
    logger.error("\n== configuration ==\n{}", conf.conf.mkString("\n"))
    logger.error("\n== parsed tree ==\n{}", parsedTree)

    val spark = SparkUtils.initSession(conf.isLocal,
      this.getClass.getSimpleName + "_" + System.currentTimeMillis())

    val data = Source.loadData(spark, conf)

    /**
      * 先序遍历parsedTree，根据节点规则绑定子节点的数据集，生成绑定后的树boundTree
      */
    val boundTree = Binder.bind(parsedTree, data)
    logger.error("\n== bound tree ==\n{}", boundTree)

    /**
      * 遍历全部叶子节点Calculation，叶子节点的数据即为不同规则下的结果集，
      * union全部叶子节点数据后，即为最终结果集
      */
    if (conf.isLocal) {
      val df = Executor.execute(boundTree)
      df.explain(extended = true)
      df.show(false)
    } else {
      Executor.executeAndSaveResultData(boundTree, conf)
    }

    spark.close()
  }
}
