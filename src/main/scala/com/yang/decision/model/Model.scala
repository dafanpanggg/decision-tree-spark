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

object Model {

  def modelReference(name: String): String = name match {
    case "XGBoost"|"xgboost" => "com.bj58.decision.model.XGBoostPipelineModel"
    case "PMML"|"pmml" => "com.bj58.decision.model.PMMLModel"
    case _ => throw new RuntimeException(s"`$name` is an unsupported model type !")
  }
}