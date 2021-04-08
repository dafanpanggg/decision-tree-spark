package com.yang.decision.tree

import com.yang.decision._
import com.yang.decision.model.Model
import org.apache.spark.sql.DataFrame

/**
  * 计算节点
  *
  * @author yangfan
  * @since 2021/3/16
  * @version 1.0.0
  */
class Calculation(parent: Tree[DataFrame],
                  ruleText: String,
                  conf: Configuration)
  extends Leaf[DataFrame](parent) {

  val rule: Rule = ruleText match {
    case r@_ if null != r =>
      val arr: Array[String] = ruleText.split(",").map(_.trim)
      arr(0) match {
        case "GeneralRule" => GeneralRule(arr(1), arr(2).toDouble)
        case "ModelRule" => ModelRule(Model.modelReference(arr(1)), arr(2), conf)
        case r@_ =>
          throw new RuntimeException(s"`$r` is an unsupported rule type !")
      }
    case _ => null
  }

  override def toString(depth: Int): String = {
    val prefix = Seq.fill(depth)('\t').mkString
    s"""Leaf[DataFrame]: {
       |${prefix + '\t'}DataFrame: $data
       |${prefix + '\t'}Rule: $rule
       |$prefix}""".stripMargin
  }
}
