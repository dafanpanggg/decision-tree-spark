package com.yang.decision.model

import com.yang.decision.{Configuration, OutputData}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, Encoders}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

/**
  * XGBoost模型
  *
  * @author yangfan
  * @since 2021/3/10
  * @version 1.0.0
  */
class XGBoostPipelineModel(modelFilePath: String, conf: Configuration)
  extends Model(modelFilePath, conf) {

  override val model: PipelineModel = if (conf.isLocal) {
    PipelineModel.load(
      "C:\\Users\\Administrator\\Desktop\\项目文档\\决策树模型工程\\age-applist-xgb-0901")
  } else {
    PipelineModel.load(modelFilePath)
  }

  override def execute(data: DataFrame): DataFrame = {
    if (null == model)
      throw new NullPointerException(
        s"`execute` method exception, `model` mast not be null !")
    if (null == data)
      throw new NullPointerException(
        s"`execute` method exception, `data` mast not be null !")

    val labeledData = data.repartition(conf.parallelism).map(l => {
      /*LabeledPoint(0d,
        Vectors.dense(
          l.getAs[mutable.WrappedArray[Double]]("vector").toArray
        ),
        l.getAs[String]("id"))*/
      /**
        * @version 2021/3/29 1.0.2 由于数组在数据传输中的效率较低，
        *          这里优化为稀疏数组，且在使用之前都以字符串的方式传递
        */
      val sparseVector = l.getAs[String]("vector")
      val vector = sparseJson2Array(sparseVector)
      LabeledPoint(0d, Vectors.dense(vector), l.getAs[String]("id"))
    })(Encoders.product[LabeledPoint])

    model.transform(labeledData)
      .select("id", "probability", "predictedLabel")
      .map(r => {
        val v = r.getAs[Vector](1)
        OutputData(r.getString(0), r.getString(2), v(v.argmax))
      })(Encoders.product[OutputData])
      .toDF()
  }

  var arr0: Array[Double] = _

  def sparseJson2Array(sparseJson: String): Array[Double] = try {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val spArr = JsonMethods.parse(sparseJson).extract[Array[Array[Double]]]
    if (null == arr0) arr0 = new Array[Double](spArr(0)(1).toInt)
    val arr1 = arr0.clone()
    spArr.tail.foreach(c => arr1(c(0).toInt) = c(1))
    arr1
  } catch {
    case _: Exception =>
      throw new RuntimeException(
        s"`sparseJson2Array` method Exception, `$sparseJson` can not parse to array !")
  }
}

case class LabeledPoint(label: Double, features: Vector, id: String)
