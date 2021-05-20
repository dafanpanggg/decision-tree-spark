package com.yang.decision.model

import com.yang.decision.{Configuration, OutputData}
import com.yang.utils.FileUtils
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
    val localModelFilePath = modelFilePath.split("/").last
    val path = FileUtils.getURLFromLocal(localModelFilePath).getPath
    PipelineModel.load(path)
  } else {
    PipelineModel.load(modelFilePath)
  }

  override def execute(data: DataFrame): DataFrame = {
    val vectorField = conf.conf("xgboost.vector.field")
    val enableSparse = conf.conf(Configuration.ENABLE_SPARSE_VECTOR).toBoolean
    execute(data, vectorField, enableSparse)
  }

  def execute(data: DataFrame, vectorField: String, isSparse: Boolean): DataFrame = {
    if (null == model)
      throw new NullPointerException(
        s"`execute` method exception, `model` mast not be null !")
    if (null == data)
      throw new NullPointerException(
        s"`execute` method exception, `data` mast not be null !")

    val labeledData = data
      /*.repartition(conf.parallelism)*/
      .map(l => {
      /**
        * @version 2021/3/29 1.0.2 由于数组在数据传输中的效率较低，
        *          这里优化为支持稀疏数组，且在使用之前都以字符串的方式传递
        */
      val sparseVector = l.getAs[String](vectorField)
      val vector = json2Array(sparseVector, isSparse)
      LabeledPoint(0d, Vectors.dense(vector), l.getAs[String]("id"))
    })(Encoders.product[LabeledPoint])

    model.transform(labeledData)
      .map(r => {
        val v = r.getAs[Vector]("probability")
        OutputData(r.getAs[String]("id"),
          r.getAs[String]("predictedLabel"), v(v.argmax), vectorField)
      })(Encoders.product[OutputData])
      .toDF()
  }

  private var arr0: Array[Double] = _

  def json2Array(jsonStr: String, sparse: Boolean): Array[Double] = try {
    implicit val formats: DefaultFormats.type = DefaultFormats
    if (sparse) {
      val spArr = JsonMethods.parse(jsonStr).extract[Array[Array[Double]]]
      if (null == arr0) arr0 = new Array[Double](spArr(0)(1).toInt)
      val arr = arr0.clone()
      spArr.tail.foreach(c => arr(c(0).toInt) = c(1))
      arr
    } else {
      JsonMethods.parse(jsonStr).extract[Array[Double]]
    }
  } catch {
    case _: Exception =>
      throw new RuntimeException(
        s"`sparseJson2Array` method Exception, `$jsonStr` can not parse to array !")
  }
}

case class LabeledPoint(label: Double, features: Vector, id: String)
