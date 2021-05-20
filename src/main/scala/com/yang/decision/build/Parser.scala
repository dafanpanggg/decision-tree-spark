package com.yang.decision.build

import com.yang.decision.Configuration
import com.yang.decision.tree._
import org.apache.spark.sql.DataFrame
import org.json4s.JsonAST.JNothing
import org.json4s.jackson.JsonMethods
import org.json4s.{DefaultFormats, JValue}

/**
  * 解析器：将配置文件解析成一棵二叉树
  *
  * @author yangfan
  * @since 2021/3/16
  * @version 1.0.0
  * @version 1.0.1 2021/5/20 修复containsKey方法无法正确检测JValue是否存在key
  */
object Parser {

  implicit val formats: DefaultFormats.type = DefaultFormats

  def parse(text: String, conf: Configuration): Tree[DataFrame] = {
    val jv = JsonMethods.parse(text)
    conf.conf ++= Seq(
      ("source.schema", (jv \ "schema").extract[String]),
      ("source", (jv \ "source").extract[String]),
      ("sink", (jv \ "sink").extract[String])
    )
    parse2Tree(jv \ "rules", null, conf)
  }

  def parse2Tree(jv: JValue,
                 parent: Tree[DataFrame],
                 conf: Configuration): Tree[DataFrame] = parse2Node(jv) match {
    case (rule, l, r) if null == l && null == r =>
      new Calculation(parent, rule, conf)
    case (rule, l, r) =>
      new Condition(parent, rule, conf,
        parse2Tree(l, null, conf), parse2Tree(r, null, conf))
    case _ => new Calculation(parent, null, conf)
  }

  def parse2Node(jv: JValue): (String, JValue, JValue) = jv match {
    case _: JValue => (
      try {
        (jv \ "rule").extract[String]
      } catch {
        case _: Exception =>
          throw new RuntimeException(
            s"`parse2Node` method Exception, `rule` mast not be null !")
      },
      if (containsKey(jv, "left")) jv \ "left" else null,
      if (containsKey(jv, "right")) jv \ "right" else null
    )
    case _ => null
  }

  def containsKey(jv: JValue, key: String): Boolean = jv \ key match {
    case JNothing => false
    case _ => true
  }
}
