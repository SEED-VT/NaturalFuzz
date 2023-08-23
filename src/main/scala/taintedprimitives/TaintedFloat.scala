package taintedprimitives

import provenance.data.{DummyProvenance, Provenance}
import symbolicexecution.{SymbolicExpression, SymbolicFloat, SymbolicTree}

import scala.reflect.runtime.universe._

/**
  * Created by malig on 4/25/19.
  */

case class TaintedFloat(override val value: Float, p:Provenance, expr: SymbolicExpression = SymbolicExpression(new SymbolicTree())) extends TaintedAny(value, p){

  def this(value: Float) = {
    this(value, DummyProvenance.create(), new SymbolicFloat(value))
  }

  def this(value: Float, p: Provenance) = {
    this(value, p, new SymbolicFloat(value, p))
  }

  def <(x: TaintedFloat): TaintedBoolean = {
    TaintedBoolean(value < x.value, newProvenance(x.getProvenance()), expr < x.expr)
  }

  /**
    * Overloading operators from here onwards
    */


  def +(x: Float): TaintedFloat = {
    val d = value + x
    TaintedFloat(d, getProvenance(), expr + x)
  }

  def -(x: Float): TaintedFloat = {
    val d = value - x
    TaintedFloat(d, getProvenance(), expr - x)
  }

  def *(x: Float): TaintedFloat = {
    val d = value * x
    TaintedFloat(d, getProvenance(), expr * x)

  }

  def /(x: Float): TaintedFloat = {
    val d = value / x
    TaintedFloat(d, getProvenance(), expr / x)
  }

  def +(x: TaintedFloat): TaintedFloat = {
    val newExpr = if(expr.length > 10) expr else if(x.expr.length > 10) x.expr else expr + x.expr
    TaintedFloat(value + x.value, mergeProvenance(getProvenance(), x.getProvenance()), newExpr)
  }


//  def +(x: TaintedDouble): TaintedDouble = {
//    TaintedDouble(value + x.value, newProvenance(x.getProvenance()))
//  }

//  def +(x: TaintedDouble): TaintedFloat = {
//    TaintedFloat(value + x.value.toFloat, mergeProvenance(getProvenance(), x.getProvenance()), expr + x.value.toFloat)
//  }

  def -(x: TaintedFloat): TaintedFloat = {
    TaintedFloat(value - x.value, mergeProvenance(getProvenance(), x.getProvenance()), expr - x.expr)
  }


  def *(x: TaintedFloat): TaintedFloat = {
    TaintedFloat(value * x.value, mergeProvenance(getProvenance(), x.getProvenance()), expr * x.expr)
  }

  def /(x: TaintedFloat): TaintedFloat = {
    TaintedFloat(value / x.value, mergeProvenance(getProvenance(), x.getProvenance()), expr / x.expr)
  }
  
  // Incomplete comparison operators - see discussion in TaintedDouble on provenance
  def >(x: TaintedFloat): TaintedBoolean = {
    TaintedBoolean(value > x.value, mergeProvenance(getProvenance(), x.getProvenance()), expr > x.expr)
  }
  
  /**
    * Operators not Supported Symbolically yet
    **/
  override def toString: String =
    value.toString + s""" (Most Influential Input Offset: ${getProvenance()})"""
//
//  def toByte: Byte = value.toByte
//
//  def toShort: Short = value.toShort
//
//  def toChar: Char = value.toChar
//
//  def toInt: Int = value.toInt
//
//  def toLong: Long = value.toLong
//
//  def toFloat: Float = value.toFloat
//
//  def toDouble: Double = value.toDouble
//
//  def unary_~ : Int = value.unary_~
//
//  def unary_+ : Int = value.unary_+
//
//  def unary_- : Int = value.unary_-
//
//  def +(x: String): String = value + x
//
//  def <<(x: Int): Int = value << x
//
//  def <<(x: Long): Int = value << x
//
//  def >>>(x: Int): Int = value >>> x
//
//  def >>>(x: Long): Int = value >>> x
//
//  def >>(x: Int): Int = value >> x
//
//  def >>(x: Long): Int = value >> x
//
//  def ==(x: Byte): Boolean = value == x
//
//  def ==(x: Short): Boolean = value == x
//
//  def ==(x: Char): Boolean = value == x
//
//  def ==(x: Long): Boolean = value == x
//
//  def ==(x: Float): Boolean = value == x
//
//  def ==(x: Double): Boolean = value == x
//
//  def !=(x: Byte): Boolean = value != x
//
//  def !=(x: Short): Boolean = value != x
//
//  def !=(x: Char): Boolean = value != x
//
//  def !=(x: Int): Boolean = value != x
//
//  def !=(x: Long): Boolean = value != x
//
//  def !=(x: Float): Boolean = value != x
//
//  def !=(x: Double): Boolean = value != x
//
//  def <(x: Byte): Boolean = value < x
//
//  def <(x: Short): Boolean = value < x
//
//  def <(x: Char): Boolean = value < x
//
//  def <(x: Int): Boolean = value < x
//
//  def <(x: Long): Boolean = value < x
//
//  def <(x: Float): Boolean = value < x
//
//  def <(x: Double): Boolean = value < x
//
//  def <=(x: Byte): Boolean = value <= x
//
//  def <=(x: Short): Boolean = value <= x
//
//  def <=(x: Char): Boolean = value <= x
//
//  def <=(x: Int): Boolean = value <= x
//
//  def <=(x: Long): Boolean = value <= x
//
//  def <=(x: Float): Boolean = value <= x
//
//  def <=(x: Double): Boolean = value <= x
//
//  def >(x: Byte): Boolean = value > x
//
//  def >(x: Short): Boolean = value > x
//
//  def >(x: Char): Boolean = value > x
//
//  def >(x: Int): Boolean = value > x
//
//  def >(x: Long): Boolean = value > x
//
//  def >(x: Float): Boolean = value > x
//
//  def >(x: Double): Boolean = value > x
//
//  def >=(x: Byte): Boolean = value >= x
//
//  def >=(x: Short): Boolean = value >= x
//
//  def >=(x: Char): Boolean = value >= x
//
//  def >=(x: Int): Boolean = value >= x
//
//  def >=(x: Long): Boolean = value >= x
//
//  def >=(x: Float): Boolean = value >= x
//
//  def >=(x: Double): Boolean = value >= x
//
//  def |(x: Byte): Int = value | x
//
//  def |(x: Short): Int = value | x
//
//  def |(x: Char): Int = value | x
//
//  def |(x: Int): Int = value | x
//
//  def |(x: Long): Long = value | x
//
//  def &(x: Byte): Int = value & x
//
//  def &(x: Short): Int = value & x
//
//  def &(x: Char): Int = value & x
//
//  def &(x: Int): Int = value & x
//
//  def &(x: Long): Long = value & x
//
//  def ^(x: Byte): Int = value ^ x
//
//  def ^(x: Short): Int = value ^ x
//
//  def ^(x: Char): Int = value ^ x
//
//  def ^(x: Int): Int = value ^ x
//
//  def ^(x: Long): Long = value ^ x
//
//  def +(x: Byte): Int = value + x
//
//  def +(x: Short): Int = value + x
//
//  def +(x: Char): Int = value + x
//
//  def +(x: Long): Long = value + x
//
//  def +(x: Float): Float = value + x
//
//  def +(x: Double): Double = value + x
//
//  def -(x: Byte): Int = value - x
//
//  def -(x: Short): Int = value - x
//
//  def -(x: Char): Int = value - x
//
//  def -(x: Long): Long = value - x
//
//  def -(x: Float): Float = value - x
//
//  def -(x: Double): Double = value - x
//
//  def *(x: Byte): Int = value * x
//
//  def *(x: Short): Int = value * x
//
//  def *(x: Char): Int = value * x
//
//  def *(x: Long): Long = value * x
//
//  def *(x: Float): Float = value * x
//
//  def *(x: Double): Double = value * x
//
//  def /(x: Byte): Int = value / x
//
//  def /(x: Short): Int = value / x
//
//  def /(x: Char): Int = value / x
//
//  def /(x: Long): Long = value / x
//
//  def /(x: Float): Float = value / x
//
//  def /(x: Double): Double = value / x
//
//  def %(x: Byte): Int = value % x
//
//  def %(x: Short): Int = value % x
//
//  def %(x: Char): Int = value % x
//
//  def %(x: Int): Int = value % x
//
//  def %(x: Long): Long = value % x
//
//  def %(x: Float): Float = value % x
//
//  def %(x: Double): Double = value % x

}

object TaintedFloat {
  implicit def lift = Liftable[TaintedFloat] { si =>
    q"(_root_.taintedprimitives.TaintedFloat(${si.value}, ${si.p}))"
  }
}