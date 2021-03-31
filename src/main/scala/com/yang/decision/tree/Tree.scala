package com.yang.decision.tree

/**
  * 二叉树抽象
  *
  * @author yangfan
  * @since 2021/3/16
  * @version 1.0.0
  */
abstract class Tree[T](parent: Tree[T]) extends Serializable {

  var data: T = _

  /**
    * 节点个数
    */
  def nodeNumber: Int = go(this, 0)

  private def go[A](tree: Tree[A], t: Int): Int = tree match {
    case Leaf(_) => t + 1
    case Branch(_, left, right) => t + 1 + go(left, 0) + go(right, 0)
    case _ => 0
  }

  /**
    * 深度值
    */
  /*val depth: Int = deepFirstSearch(this)(_ => 1)(_ max _ + 1)*/

  /**
    * 转换操作
    */
  /*def map[A, B](tree: Tree[A])(f: A => B): Tree[B] =
    deepFirstSearch(tree)(x => f(x))((a, b) => Branch(a, b))*/

  /**
    * 深度优先遍历
    */
  /*def deepFirstSearch[A, B](tree: Tree[A])(f: A => B)(g: (B, B) => B): B = tree match {
    case l@Leaf(_) => f(l.data)
    case b@Branch(_, left, right) =>
      g(deepFirstSearch(left), deepFirstSearch(right))
      f(b.data)
  }*/

  override def toString: String = toString(0)

  def toString(depth: Int): String = super.toString
}

/**
  * 叶子节点
  *
  * @since 2021/3/16
  * @version 1.0.0
  */
case class Leaf[T](parent: Tree[T]) extends Tree[T](parent)

/**
  * 分支节点
  *
  * @since 2021/3/16
  * @version 1.0.0
  */
case class Branch[T](parent: Tree[T], left: Tree[T], right: Tree[T]) extends Tree[T](parent)
