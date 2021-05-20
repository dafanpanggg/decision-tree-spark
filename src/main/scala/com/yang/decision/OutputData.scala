package com.yang.decision

/**
  * 输出对象
  *
  * @author yangfan
  * @since 2021/3/16
  * @version 1.0.0
  * @version 1.0.4 增加source字段，用于区分特征值的来源
  */
case class OutputData(id: String,
                      featureValue: String,
                      confidenceScore: Double,
                      source: String)
