package com.yang

import com.yang.decision.ApplicationContext
import org.apache.spark.internal.Logging

import scala.collection.mutable

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
  * @version 2021/4/8 1.0.3 增加了节点对PMML的支持，
  *          增加Model实现类[[com.yang.decision.model.PMMLModel]]
  * @version 2021/4/19 1.0.4 增加source字段，用于区分特征计算的数据来源
  * @version 2021/4/26 1.0.5 对[[com.yang.DecisionTreeApplication]]入口类进行封装，
  *          提供外部开发测试的上下文[[com.yang.decision.ApplicationContext]]，
  *          示例：
  *          val conf = Seq(
  *             (Configuration.PROCESS_TIME, String.valueOf(1)),
  *             (Configuration.IS_LOCAL, String.valueOf(true)),
  *             (Configuration.IS_CACHE, String.valueOf(false)),
  *             (Configuration.ENABLE_SPARSE_VECTOR, String.valueOf(false)),
  *             ("local.source", "car.csv")
  *          )
  *          val context = ApplicationContext
  *             .builder
  *             .config(conf)
  *             .args(args)
  *             .create()
  *          context.run()
  */
object DecisionTreeApplication {

  def builder = new Builder

  class Builder extends Logging {
    lazy val options = new mutable.HashMap[String, String]
    var args: Array[String] = _

    def config(conf: Seq[(String, String)]): Builder = {
      logError(s"\n== input conf ==\n$conf")
      options ++= conf
      this
    }

    private def readInputArgs(args: Array[String]): Seq[(String, String)] = {
      logError(s"\n== input args ==\n$args")
      if (null == args || args.length < 1) {
        throw new IllegalArgumentException(s"args mast not be null !")
      }
      parseArgs(args.toList)
    }

    private def parseArgs(args: List[String]): Seq[(String, String)] = args match {
      case k :: v :: tail =>
        if (k.startsWith("--")) {
          (k.substring(2), v) +: parseArgs(tail)
        } else {
          throw new IllegalArgumentException(
            s"args key mast starts with `--` but get `$k` !")
        }
      case Nil => List()
    }

    def args(args: Array[String]): Builder = {
      options ++= readInputArgs(args)
      this
    }

    def create(): ApplicationContext = ApplicationContext(options.toSeq)
  }

  def main(args: Array[String]): Unit = builder.args(args).create().run()
}
