package taintedprimitives

/**
  * Created by malig on 4/25/19.
  */

import provenance.data.{DummyProvenance, Provenance}
import symbolicexecution.{ConcreteValueNode, SymbolicExpression, SymbolicTree}

import scala.reflect.runtime.universe._

case class TaintedBoolean(override val value: Boolean, p : Provenance, symbolicExpression: SymbolicExpression) extends TaintedAny(value, p) {
  def this(value: Boolean) = {
    this(value, DummyProvenance.create(), SymbolicExpression(new SymbolicTree(new ConcreteValueNode("SymBool", value))))
  }
  
  /**
    * Overloading operators from here onwards
    */

  def &&(b: TaintedBoolean): TaintedBoolean = {
    TaintedBoolean(value && b.value, newProvenance(b.getProvenance()), symbolicExpression.and(b.symbolicExpression))
  }

  def &&(b: Boolean): TaintedBoolean = {
    TaintedBoolean(value && b, newProvenance(getProvenance()), symbolicExpression)
  }

  def ||(b: TaintedBoolean): TaintedBoolean = {
    TaintedBoolean(value || b.value, newProvenance(b.getProvenance()), symbolicExpression.or(b.symbolicExpression))
  }

  def ||(b: Boolean): TaintedBoolean = {
    TaintedBoolean(value || b, newProvenance(getProvenance()), symbolicExpression)
  }
}
