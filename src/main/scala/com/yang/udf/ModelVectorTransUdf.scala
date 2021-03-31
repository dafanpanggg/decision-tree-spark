package com.yang.udf

import javax.annotation.Nonnull
import org.apache.spark.sql.api.java.UDF3

import scala.collection.mutable

/**
  * 遍历传入数组，将其转为目标向量数组（XGBoost模型）
  *
  * @author yangfan
  * @since 2021/3/18
  * @version 1.0.0
  */
class ModelVectorTransUdf extends UDF3[String, String, String, Seq[Double]] {

  private var targetMap: mutable.LinkedHashMap[Any, Double] = _

  override def call(t1: String,
                    @Nonnull t2: String,
                    @Nonnull t3: String): Seq[Double] =
    evaluate(t1,t2,t3)

  def evaluate(str1: String,
               @Nonnull str2: String,
               @Nonnull regex: String): Seq[Double] = {
    if(null == str1 || str1.length == 0) return null
    targetMap match {
      case i@_ if null != i =>
        evaluate(str1.split(regex), null)
      case _ =>
        evaluate(str1.split(regex), str2.split(regex))
    }
  }

  def evaluate(arr1: Seq[Any], @Nonnull arr2: Seq[Any]): Seq[Double] = {
    if (null == targetMap) {
      targetMap = new mutable.LinkedHashMap[Any, Double]()
      arr2.foreach(i => targetMap += ((i, 0d)))
    }
    arr1 match {
      case s@_ if null != s =>
        val m = new mutable.LinkedHashMap[Any, Double]() ++= targetMap
        s.foreach(i => if (m.contains(i)) m(i) = 1d)
        m.values.toSeq
      case _ => null
    }
  }
}

object ModelVectorTransUdf {

  def main(args: Array[String]): Unit = {
    val result = new ModelVectorTransUdf()
        .call("1,6", "1,2,3,4,5,6", ",")
    println(result.mkString(","))
  }
}
