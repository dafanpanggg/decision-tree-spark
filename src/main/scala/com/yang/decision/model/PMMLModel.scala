package com.yang.decision.model

import com.yang.decision.{Configuration, OutputData}
import com.yang.utils.FileUtils
import org.apache.spark.ml.Transformer
import org.apache.spark.mllib.linalg.DenseVector
import org.apache.spark.sql.{DataFrame, Encoders}
import org.jpmml.evaluator.spark.TransformerBuilder
import org.jpmml.evaluator.{DefaultVisitorBattery, LoadingModelEvaluatorBuilder}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization

/**
  * PMML模型
  *
  * PMML模型加载使用开源的jpmml-evaluator-spark组件
  * @see https://github.com/jpmml/jpmml-sparkml
  *
  * @author yangfan
  * @since 2021/4/7
  * @version 1.0.0 目前仅仅只支持输出0,1模型分
  */
class PMMLModel(modelFilePath: String, conf: Configuration)
  extends Model(modelFilePath, conf) {

  override val model: Transformer = {
    val is = if (conf.isLocal){
      FileUtils.getInputStreamFromLocal("bmdlv4_xgbv7_20210312.xml")
    } else {
      FileUtils.getInputStreamFromHDFS(modelFilePath)
    }

    val evaluator = new LoadingModelEvaluatorBuilder()
      .setLocatable(false)
      .setVisitors(new DefaultVisitorBattery())
      .load(is)
      .build()

    new TransformerBuilder(evaluator)
      .withProbabilityCol("probability")
      .exploded(true)
      .build()
  }

  override def execute(data: DataFrame): DataFrame = {
    model.transform(data)
      .map(r => {
        implicit val formats: DefaultFormats.type = DefaultFormats
        val v = r.getAs[DenseVector]("probability").values
        val vStr = Serialization.write(v)
        OutputData(r.getAs[String]("id"), vStr, Double.NaN)
      })(Encoders.product[OutputData])
      .toDF()
  }
}
