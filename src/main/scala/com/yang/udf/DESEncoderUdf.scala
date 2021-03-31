package com.yang.udf

import com.yang.util.DESEncoder
import javax.annotation.Nonnull
import org.apache.hadoop.hive.ql.metadata.HiveException
import org.apache.spark.sql.api.java.UDF2

/**
  * DES加密解密UDF
  *
  * @author yangfan
  * @since 2021/3/22
  * @version 1.0.0
  */
class DESEncoderUdf extends UDF2[String, String, String] {

  override def call(t1: String, t2: String): String = evaluate(t1, t2)

  def evaluate(str: String, @Nonnull flag: String): String = try {
    flag match {
      case "encode" =>
        DESEncoder.encode(str)
      case "decode" =>
        DESEncoder.decode(str)
      case _ => throw new HiveException(
        s"DESEncoder second param mast be `encode` or `decode` but get `$flag` !")
    }
  } catch {
    case e: Exception => throw new HiveException(e)
  }
}

object DESEncoderUdf {
  def main(args: Array[String]): Unit = {
    val udf = new DESEncoderUdf()
    val encodeStr = udf.call("510821198211184818", "encode")
    val decodeStr = udf.call(encodeStr, "decode")
    println(encodeStr)
    println(decodeStr)
  }
}
