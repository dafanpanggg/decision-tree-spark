package com.yang.utils

import java.io.{BufferedReader, InputStream, InputStreamReader}
import java.net.URL

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

  def getURLFromLocal(src: String): URL =
    this.getClass.getClassLoader.getResource(src)

  def getInputStreamFromLocal(src: String): InputStream =
    getURLFromLocal(src).openStream()

  def getInputStreamFromHDFS(src: String): InputStream =
    FileSystem.get(new Configuration).open(new Path(src))

  def readFromLocal(src: String): String = try {
    val in = getInputStreamFromLocal(src)
    val buffer = Source.fromInputStream(in)
    val str = buffer.getLines.mkString("\n")
    in.close()
    buffer.close()
    str
  } catch {
    case _: Exception =>
      throw new RuntimeException(
        s"`readFromFile` method exception, read configuration file failed !")
  }

  def readFromHDFS(src: String): String = try {
    val in = getInputStreamFromHDFS(src)
    val br = new BufferedReader(new InputStreamReader(in))
    val arr = new mutable.ArrayBuffer[String]()
    var line = br.readLine()
    while (null != line) {
      arr += line
      line = br.readLine()
    }
    in.close()
    br.close()
    arr.mkString("\n")
  } catch {
    case _: Exception =>
      throw new RuntimeException(
        s"`readFromHDFS` method exception, read configuration file failed !")
  }

  def main(args: Array[String]): Unit = {
    println(getURLFromLocal("AgeGroupRule.json").getPath)
  }
}
