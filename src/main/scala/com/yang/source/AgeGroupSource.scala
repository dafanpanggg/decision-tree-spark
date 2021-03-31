package com.yang.source

import com.yang.decision.Source
import com.yang.udf.{DESEncoderUdf, ModelVectorTransUdf}
import org.apache.spark.sql.types.{ArrayType, DataTypes}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * explain
  *
  * @author yangfan
  * @since 2021/3/18
  * @version 1.0.0
  */
class AgeGroupSource(spark: SparkSession,
                     processDay: String) extends Source(spark, processDay) {

  val APP_GiNi_CONF = "4399游戏盒|代练通|b612咔叽|wifi万能钥匙|掌上wegame|翼支付|个人所得税|掌上英雄联盟|比心|饿了么|三国杀|知到|inshot-视频编辑|手机淘宝|美图秀秀|节奏大师|识货|部落冲突|qq|前程无忧51job|百度极速版|美柚|一甜相机|虎牙直播|迷你世界|云班课|轻颜相机|tt语音|火山小视频|驾考宝典|买单吧|宝宝树孕育|一起小学学生| 360手机卫士|家长通|同桌游戏|vsco|意见反馈|picsart美易照片编辑|哔哩哔哩|爱奇艺|一起作业学生|安全教育平台|小猿口算|熊猫优选|y2002电音|滴滴车主|百度网盘|dnf助手|作业帮|剪映|毒|微光|纳米盒|qq音乐|欢乐斗地主|汽车之家|uu加速器|最右|steam|内涵段子|小天才|百度手机助手|店长直聘|美篇|好看视频|uki|云课堂智慧职教|一键锁屏|蓝墨云班课|360手机助手|学习通|掌上飞车|抖音插件|支付宝|萤石云视频|全民小视频|小红书|手机营业厅|yy|铃声多多|腾讯动漫|来分期|快手|发现精彩|今日头条极速版|捷信金融|交易猫|faceu激萌|天天酷跑|蘑菇街|中国大学mooc|斗米|同花顺|分期乐|企鹅电竞|一起学|学而思网校|抖音短视频|快手直播伴侣|芒果tv|酷狗音乐|平安口袋银行|应用宝|绝地求生刺激战场|网易云音乐|易班|qq飞车|好游快爆|微博|王者营地|影视大全|球球大作战|boss直聘|皮皮虾|作业盒子小学|天天p图|小盒家长|皮皮搞笑|u净|全球购骑士特权|小猿搜题|mix|momo陌陌|掌上穿越火线|今日头条|超级课程表|穿越火线：枪战王者|掌通家园|学小易|王者荣耀|pubgmobile|工银融e联|快手小游戏|知乎|小恩爱|心悦俱乐部|平安普惠|百度贴吧|优酷|keep|美团|得物(毒)|百词斩|找靓机|晓黑板|到梦空间|美团外卖|智联招聘|华为主题动态引擎|情侣空间|抖音火山版|闲鱼|中国建设银行|今日校园|玩吧|相册管家|便签|智学网|浦发信用卡|熊猫直播|交管12123|掌上生活|腾讯新闻|猫咪|迅雷|探探|腾讯欢乐麻将|qq同步助手|凤凰新闻|平安好车主|和平精英|比心陪练|优酷视频|快猫|狼人杀|第五人格|快看漫画|斗鱼直播|途虎养车|搜狗输入法|西瓜视频|快影|人人视频|学习强国|倒数日|韩剧tv|音遇|soul|菜鸟裹裹|云电脑|无他相机|迅游手游加速器|租号玩|平安金管家|qq安全中心|360清理大师|葫芦侠|王者荣耀助手|taptap|全民k歌|腾讯视频|唱鸭|掌上道聚城|百度"

  //override def loadData(): DataFrame = getInputDataLocal(spark)
  override def loadData(): DataFrame = getInputData(spark)

  private def getInputDataLocal(spark: SparkSession): DataFrame = {
    spark.read.format("csv")
      .option("header", "true")
      .option("multiLine", value = true)
      .option("quote", "\"")
      .option("escape", "\"")
      .load("C:\\Users\\Administrator\\Desktop\\age.csv")
  }

  private def getInputData(spark: SparkSession): DataFrame = {
    spark.udf.register("des_encoder",
      new DESEncoderUdf, DataTypes.StringType)
    spark.udf.register("vector_trans",
      new ModelVectorTransUdf, ArrayType(DataTypes.DoubleType))
    spark.sql(
      s"""
         |select
         |    userid as id
         |    ,jr_age
         |    ,xinan_age
         |    ,jl_age
         |    ,umc_age
         |    ,yc_age
         |    ,vector
         |from xxx
       """.stripMargin
    )
  }
}
