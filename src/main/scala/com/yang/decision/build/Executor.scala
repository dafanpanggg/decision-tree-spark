package com.yang.decision.build

import com.yang.decision.Configuration
import com.yang.decision.tree._
import org.apache.spark.sql.DataFrame

/**
  * 执行器：将Tree转换成Spark可执行的物理执行计划
  *
  * @author yangfan
  * @since 2021/3/16
  * @version 1.0.0
  */
object Executor {

  def executeAndSaveResultData(tree: Tree[DataFrame],
                               conf: Configuration): Unit = {
    val spark = tree.data.sparkSession
    execute(tree).createOrReplaceTempView("output_data_temp_view")
    val sink = conf.conf("sink").split(",").map(_.trim)
    spark.sql(
      s"""
         |insert overwrite table ${sink(0)}
         |partition(dt = '${conf.processTime}', model = '${sink(1)}')
         |select
         |	id
         |	,featureValue
         |	,confidenceScore
         |from output_data_temp_view
       """.stripMargin
    )
  }

  def execute(boundTree: Tree[DataFrame]): DataFrame = boundTree match {
    case leaf: Calculation if null != leaf.rule => leaf.data
    case branch: Condition => union(execute(branch.left), execute(branch.right))
    case _ => null
  }

  def union(l: DataFrame, r: DataFrame): DataFrame = (l, r) match {
    case _ if null == l => r
    case _ if null == r => l
    case _ => l.union(r)
  }
}
