package com.yang.decision.tree

import com.ql.util.express.{DefaultContext, ExpressRunner}
import com.yang.decision.Configuration
import com.yang.express.EqualsOperator
import org.apache.spark.sql.{DataFrame, Row}

/**
  * 条件节点
  *
  * @author yangfan
  * @since 2021/3/16
  * @version 1.0.0
  */
class Condition(parent: Tree[DataFrame],
                ruleText: String,
                conf: Configuration,
                left: Tree[DataFrame],
                right: Tree[DataFrame])
  extends Branch[DataFrame](parent, left, right) {

  lazy val runner: ExpressRunner = {

    val runner = new ExpressRunner()

    /**
      * 修改原表达式 `==` 的判断逻辑
      * 修改前：null == null 返回：true
      * 修改后：null == null 返回：false
      */
    runner.replaceOperator("==", new EqualsOperator())
    runner
  }

  lazy val fields: Array[String] = runner.getOutVarNames(ruleText)

  lazy val leftDF: DataFrame = data.filter(r => isLeft(r))

  lazy val rightDF: DataFrame = data.filter(!isLeft(_))

  def isLeft(row: Row): Boolean = {
    val context = new DefaultContext[String, AnyRef]
    fields.foreach(field =>
      context.put(field, row.getAs(field).asInstanceOf[AnyRef])
    )
    try {
      runner.execute(ruleText, context,
        null, true, false)
        .asInstanceOf[Boolean]
    } catch {
      case _: Exception =>
        throw new RuntimeException(
          s"`isLeft` method Exception, condition expression compute failed !")
    }
  }

  override def toString(depth: Int): String = {
    val prefix = Seq.fill(depth)('\t').mkString
    s"""Branch[DataFrame]: {
       |${prefix + '\t'}DataFrame: $data
       |${prefix + '\t'}Condition: $ruleText
       |${prefix + '\t'}LeftNode: ${if (null == left) "null" else left.toString(depth + 1)}
       |${prefix + '\t'}RightNode: ${if (null == right) "null" else right.toString(depth + 1)}
       |$prefix}""".stripMargin
  }
}
