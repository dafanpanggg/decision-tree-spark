package com.yang.decision.build

import com.yang.decision.tree._
import org.apache.spark.sql.DataFrame

/**
  * 绑定器：
  *   1、传入解析后的二叉树parsedTree，和根节点的数据集rootDataFrame
  *   2、绑定根节点的数据集parsedTree.data = dataFrame
  *   3、递归遍历左右子节点，绑定各子节点的数据集
  *     如果该节点是分支节点（Condition），则根据条件表达式分别绑定左右节点的数据集，
  *       再递归绑定左右节点及其子节点的数据集，返回该节点：
  *       branch.left.data = branch.leftDF
  *       branch.right.data = branch.rightDF
  *     如果该节点是叶子节点（Calculation），则根据计算规则rule，得到该节点的数据集，返回该节点：
  *       leaf.data = leaf.rule.execute(leaf.data)
  *   4、如果该节点既不是分支节点，也不是叶子节点，则不做绑定直接返回该节点
  *
  * @author yangfan
  * @since 2021/3/16
  * @version 1.0.0
  */
object Binder {

  def bind(parsedTree: Tree[DataFrame], dataFrame: DataFrame): Tree[DataFrame] = {
    parsedTree.data = dataFrame
    bindChild(parsedTree)
  }

  def bindChild(parsedTree: Tree[DataFrame]): Tree[DataFrame] = parsedTree match {
    case leaf: Calculation if null != leaf.rule =>
      leaf.data = leaf.rule.execute(leaf.data)
      leaf
    case branch: Condition =>
      branch.left.data = branch.leftDF
      branch.right.data = branch.rightDF
      bindChild(branch.left)
      bindChild(branch.right)
      branch
    case other@_ => other
  }
}
