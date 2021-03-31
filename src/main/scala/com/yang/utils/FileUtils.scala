package com.yang.utils

import java.io.{BufferedReader, InputStreamReader}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable
import scala.io.Source

/**
  * explain
  *
  * @author yangfan
  * @since 2021/3/2
  * @version 1.0.0
  */
object FileUtils {

  def readFromLocal(src: String): String = try {
    val filePath = this.getClass.getClassLoader.getResource(src)
    val buffer = Source.fromURL(filePath)
    val str = buffer.getLines.mkString("\n")
    buffer.close()
    str
  } catch {
    case _: Exception =>
      throw new RuntimeException(
        s"`readFromFile` method exception, read configuration file failed !")
  }

  def readFromHDFS(src: String): String = try {
    val fs = FileSystem.get(new Configuration)
    val br = new BufferedReader(new InputStreamReader(fs.open(new Path(src))))
    val arr = new mutable.ArrayBuffer[String]()
    var line = br.readLine()
    while (null != line) {
      arr += line
      line = br.readLine()
    }
    br.close()
    fs.close()
    arr.mkString("\n")
  } catch {
    case _: Exception =>
      throw new RuntimeException(
        s"`readFromHDFS` method exception, read configuration file failed !")
  }

  def main(args: Array[String]): Unit = {
    println(readFromLocal("AgeGroupRule.json"))
  }
}
