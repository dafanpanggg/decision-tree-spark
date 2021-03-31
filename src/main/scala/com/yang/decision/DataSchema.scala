package com.yang.decision

/**
  * 配置属性
  *
  * @author yangfan
  * @since 2021/3/18
  * @version 1.0.0
  */
case class ModelProperties(schema: Array[String],
                           input: Array[String],
                           output: Array[String]) {
  override def toString: String =
    s"""schema: ${schema.mkString(",")}
       |source: ${input.mkString(",")}
       |output: ${output.mkString(",")}
       |""".stripMargin
}

/**
  * 输出对象
  *
  * @since 2021/3/16
  * @version 1.0.0
  */
case class OutputData(id: String,
                      featureValue: String,
                      confidenceScore: Double)
