package com.yang.decision

import com.yang.decision.build._
import com.yang.utils._
import org.apache.spark.internal.Logging

/**
  * 决策树模型上下文
  *
  * @author yangfan
  * @since 2021/4/26
  */
case class ApplicationContext(options: Seq[(String, String)]) extends Logging {

  def run(): Unit = {
    /**
      * 加载默认配置以及加载自定义配置
      */
    val conf = Configuration.init(options)

    val modelConfigText = if (conf.isLocal) {
      FileUtils.readFromLocal(conf.modelConfFilePath.split("/").last)
    } else {
      FileUtils.readFromHDFS(conf.modelConfFilePath)
    }
    logError(s"\n== model config text ==\n$modelConfigText")

    /**
      * 解析Json为一棵二叉树，同时绑定树节点规则
      */
    val parsedTree = Parser.parse(modelConfigText, conf)
    logError(s"\n== configuration ==\n${conf.conf.mkString("\n")}")
    logError(s"\n== parsed tree ==\n$parsedTree")

    val spark = SparkUtils.initSession(conf.isLocal,
      this.getClass.getSimpleName + "_" + System.currentTimeMillis())

    val data = Source.loadData(spark, conf)

    /**
      * 先序遍历parsedTree，根据节点规则绑定子节点的数据集，生成绑定后的树boundTree
      */
    val boundTree = Binder.bind(parsedTree, data)
    logError(s"\n== bound tree ==\n$boundTree")

    /**
      * 遍历全部叶子节点Calculation，叶子节点的数据即为不同规则下的结果集，
      * union全部叶子节点数据后，即为最终结果集
      */
    if (conf.isLocal) {
      val df = Executor.execute(boundTree)
      df.explain(extended = true)
      df.show(1000, truncate = false)
    } else {
      Executor.executeAndSaveResultData(boundTree, conf)
    }

    spark.close()
  }
}
