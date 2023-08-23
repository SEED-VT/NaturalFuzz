package symbolicexecution

import abstraction.BaseRDD
import runners.Config
import utils.{Query, RDDLocations}

case class SymbolicExpression (expr: SymbolicTree) extends Serializable {



  def length: Int = {
    expr.height
  }

  def or(symbolicExpression: SymbolicExpression): SymbolicExpression = {
    SymbolicExpression(SymbolicTree(expr, new OperationNode("||"), symbolicExpression.expr))
  }

  def and(symbolicExpression: SymbolicExpression): SymbolicExpression = {
    SymbolicExpression(SymbolicTree(expr, new OperationNode("&&"), symbolicExpression.expr))
  }

  def +(x: SymbolicExpression): SymbolicExpression = {
    SymbolicExpression(SymbolicTree(expr, new OperationNode("+"), x.expr))
  }

  def +(x: Int): SymbolicExpression = {
    SymbolicExpression(SymbolicTree(expr, new OperationNode("+"), new SymbolicInteger(x).expr))
  }

  def +(x: Float): SymbolicExpression = {
    SymbolicExpression(SymbolicTree(expr, new OperationNode("+"), new SymbolicFloat(x).expr))
  }

  def -(x: SymbolicExpression): SymbolicExpression = {
    SymbolicExpression(SymbolicTree(expr, new OperationNode("-"), x.expr))
  }

  def -(x: Int): SymbolicExpression = {
    SymbolicExpression(SymbolicTree(expr, new OperationNode("-"), new SymbolicInteger(x).expr))
  }

  def -(x: Float): SymbolicExpression = {
    SymbolicExpression(SymbolicTree(expr, new OperationNode("-"), new SymbolicFloat(x).expr))
  }
  def *(x: SymbolicExpression): SymbolicExpression = {
    SymbolicExpression(SymbolicTree(expr, new OperationNode("*"), x.expr))
  }

  def *(x: Int): SymbolicExpression = {
    SymbolicExpression(SymbolicTree(expr, new OperationNode("*"), new SymbolicInteger(x).expr))
  }

  def *(x: Float): SymbolicExpression = {
    SymbolicExpression(SymbolicTree(expr, new OperationNode("*"), new SymbolicFloat(x).expr))
  }

  def /(x: SymbolicExpression): SymbolicExpression = {
    SymbolicExpression(SymbolicTree(expr, new OperationNode("/"), x.expr))
  }

  def /(x: Int): SymbolicExpression = {
    SymbolicExpression(SymbolicTree(expr, new OperationNode("/"), new SymbolicInteger(x).expr))
  }

  def /(x: Float): SymbolicExpression = {
    SymbolicExpression(SymbolicTree(expr, new OperationNode("/"), new SymbolicFloat(x).expr))
  }

  def <(x: SymbolicExpression): SymbolicExpression = {
    SymbolicExpression(SymbolicTree(expr, new OperationNode("<"), x.expr))
  }

  def <(x: Int): SymbolicExpression = {
    SymbolicExpression(SymbolicTree(expr, new OperationNode("<"), new SymbolicInteger(x).expr))
  }

  def <=(x: SymbolicExpression): SymbolicExpression = {
    SymbolicExpression(SymbolicTree(expr, new OperationNode("<="), x.expr))
  }

  def >(x: SymbolicExpression): SymbolicExpression = {
    SymbolicExpression(SymbolicTree(expr, new OperationNode(">"), x.expr))
  }

  def >=(x: SymbolicExpression): SymbolicExpression = {
    SymbolicExpression(SymbolicTree(expr, new OperationNode(">="), x.expr))
  }

  def ==(x: SymbolicExpression): SymbolicExpression = {
    SymbolicExpression(SymbolicTree(expr, new OperationNode("=="), x.expr))
  }

  def ==(x: Int): SymbolicExpression = {
    SymbolicExpression(SymbolicTree(expr, new OperationNode("=="), new SymbolicInteger(x).expr))
  }

  def ==(x: String): SymbolicExpression = {
    SymbolicExpression(SymbolicTree(expr, new OperationNode("=="), new SymbolicString(x).expr))
  }

  def toCNF: SymbolicExpression = {
    println("Converting to CNF")
    this //TODO: Convert expression to CNF
  }

  def removeProv: SymbolicExpression = {
    SymbolicExpression(expr.removeProv)
  }

  def toQueries: List[Query] = {
    val atomicExpressions = expr.breakIntoAtomic
    atomicExpressions.map(_.toQuery)
  }

  override def toString: String = expr.toString

  def isEmpty: Boolean = expr.isEmpty
}
