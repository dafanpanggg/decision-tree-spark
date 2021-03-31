package com.yang.decision

import com.yang.decision.model.Model
import org.apache.spark.sql.{DataFrame, Encoders}

/**
  * 计算规则抽象
  *
  * @author yangfan
  * @since 2021/3/16
  * @version 1.0.0
  */
trait Rule extends Serializable {

  def execute(data: DataFrame): DataFrame
}

/**
  * 普通计算
  *
  * @since 2021/3/16
  * @version 1.0.0
  */
case class GeneralRule(fieldName: String, confidence: Double) extends Rule {

  override def execute(data: DataFrame): DataFrame = {
    data.map(row =>
      OutputData(
        row.getAs[String]("id"),
        row.getAs[String](fieldName),
        confidence
      )
    )(Encoders.product[OutputData]).toDF()
  }

  override def toString: String = s"GeneralRule[fieldName: $fieldName, confidence: $confidence]"
}

/**
  * 模型计算
  *
  * @since 2021/3/16
  * @version 1.0.0
  */
case class ModelRule(modelClass: String,
                     modelFilePath: String,
                     conf: Configuration) extends Rule {

  lazy val model: Model = Class.forName(modelClass)
    .getConstructor(classOf[String], classOf[Configuration])
    .newInstance(modelFilePath, conf)
    .asInstanceOf[Model]

  override def execute(data: DataFrame): DataFrame = model.execute(data)

  override def toString: String = s"ModelRule[modelClass: $modelClass, " +
    s"modelFilePath: $modelFilePath]"
}
