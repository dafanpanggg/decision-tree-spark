package com.yang.decision.model

import com.yang.decision.{Configuration, OutputData}
import com.yang.utils.FileUtils
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{DataFrame, Encoders}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

import scala.collection.mutable

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
    val path = FileUtils.getURLFromLocal("age-applist-xgb-0901").getPath
    PipelineModel.load(path)
  } else {
    PipelineModel.load(modelFilePath)
  }

  override def execute(data: DataFrame): DataFrame = {
    val enableSparse = conf.conf(Configuration.ENABLE_SPARSE_VECTOR).toBoolean
    execute(data, enableSparse)
  }

  def execute(data: DataFrame, enableSparse: Boolean): DataFrame = {
    if (null == model)
      throw new NullPointerException(
        s"`execute` method exception, `model` mast not be null !")
    if (null == data)
      throw new NullPointerException(
        s"`execute` method exception, `data` mast not be null !")

    val labeledData = data.repartition(conf.parallelism).map(l => {
      /**
        * @version 2021/3/29 1.0.2 由于数组在数据传输中的效率较低，
        *          这里优化为支持稀疏数组，且在使用之前都以字符串的方式传递
        */
      if (enableSparse) {
        val sparseVector = l.getAs[String]("vector")
        val vector = sparseJson2Array(sparseVector)
        LabeledPoint(0d, Vectors.dense(vector), l.getAs[String]("id"))
      } else {
        val v = l.getAs[mutable.WrappedArray[Double]]("vector")
        val vArr = Vectors.dense(v.toArray)
        LabeledPoint(0d, vArr, l.getAs[String]("id"))
      }
    })(Encoders.product[LabeledPoint])

    model.transform(labeledData)
      .map(r => {
        val v = r.getAs[Vector]("probability")
        OutputData(r.getAs[String]("id"),
          r.getAs[String]("predictedLabel"), v(v.argmax))
      })(Encoders.product[OutputData])
      .toDF()
  }

  private var arr0: Array[Double] = _

  def sparseJson2Array(sparseJson: String): Array[Double] = try {
    implicit val formats: DefaultFormats.type = DefaultFormats
    val spArr = JsonMethods.parse(sparseJson).extract[Array[Array[Double]]]
    if(null == arr0) arr0 = new Array[Double](spArr(0)(1).toInt)
    val arr = arr0.clone()
    spArr.tail.foreach(c => arr(c(0).toInt) = c(1))
    arr
  } catch {
    case _: Exception =>
      throw new RuntimeException(
        s"`sparseJson2Array` method Exception, `$sparseJson` can not parse to array !")
  }
}

case class LabeledPoint(label: Double, features: Vector, id: String)
