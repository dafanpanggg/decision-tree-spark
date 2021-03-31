package com.yang.decision.model

import com.yang.decision.Configuration
import org.apache.spark.sql.DataFrame

/**
  * 模型抽象
  *
  * @author yangfan
  * @since 2021/3/8
  * @version 1.0.0
  */
abstract class Model(modelFilePath:String, conf: Configuration) extends Serializable {

  val model: Any

  def execute(data: DataFrame): DataFrame
}