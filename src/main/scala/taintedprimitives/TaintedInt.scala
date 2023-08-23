package taintedprimitives

/**
  * Created by malig on 4/25/19.
  */

import provenance.data.{DummyProvenance, Provenance}
import symbolicexecution.{SymbolicExpression, SymbolicInteger, SymbolicTree}

import scala.reflect.runtime.universe._

case class TaintedInt(override val value: Int, p : Provenance, expr: SymbolicExpression = new SymbolicExpression(new SymbolicTree())) extends TaintedAny(value, p) {
  def this(value: Int) = {
    this(value, DummyProvenance.create(), new SymbolicInteger(value))
  }

  def this(value: Int, p: Provenance) = {
    this(value, p, new SymbolicInteger(value))
  }
  
  /**
    * Overloading operators from here onwards
    */
  def +(x: Int): TaintedInt = {
    val d = value + x
    TaintedInt(d, getProvenance(), expr + x)
  }

  def -(x: Int): TaintedInt = {
    val d = value - x
    TaintedInt(d, getProvenance(), expr - x)
  }

  def *(x: Int): TaintedInt = {
    val d = value * x
    TaintedInt(d, getProvenance(), expr * x)
  }

  def *(x: Float): TaintedFloat = {
    val d = value * x
    TaintedFloat(d, getProvenance())
  }

  def /(x: Int): TaintedDouble = {
    val d = value / x
    TaintedDouble(d, getProvenance())
  }

  def /(x: Long): TaintedDouble= {
    val d = value / x
    TaintedDouble(d, getProvenance())
  }

  def /(x: TaintedInt): TaintedInt = {
    TaintedInt(value / x.value, mergeProvenance(getProvenance(), x.getProvenance()), expr / x.expr)
  }


  def +(x: TaintedInt): TaintedInt = {
    TaintedInt(value + x.value, mergeProvenance(getProvenance(), x.getProvenance()), expr + x.expr)
  }

  def -(x: TaintedInt): TaintedInt = {
    TaintedInt(value - x.value, mergeProvenance(getProvenance(), x.getProvenance()), expr - x.expr)
  }

  def *(x: TaintedInt): TaintedInt = {
    TaintedInt(value * x.value, mergeProvenance(getProvenance(), x.getProvenance()), expr * x.expr)
  }

  def %(x: Int): TaintedInt = {
    new TaintedInt(value % x, getProvenance())
  }
  
  // Implementing on a need-to-use basis
  def toInt: TaintedInt = this
  def toDouble: TaintedDouble = TaintedDouble(value.toDouble, getProvenance())

  /**
    * Operators not supported yet
    */

  def ==(x: Int): TaintedBoolean = {
    TaintedBoolean(value == x, getProvenance(), expr == x)
  }

  def ==(x: TaintedInt): TaintedBoolean = {
    TaintedBoolean(value == x.value, newProvenance(x.getProvenance()), expr == x.expr)
  }

  def toByte: Byte = value.toByte

  def toShort: Short = value.toShort

  def toChar: Char = value.toChar

  

  def toLong: Long = value.toLong

  def toFloat: TaintedFloat = new TaintedFloat(value.toFloat, p)

  //def toDouble: Double = value.toDouble

  def unary_~ : Int = value.unary_~

  def unary_+ : Int = value.unary_+

  def unary_- : Int = value.unary_-

  def +(x: String): String = value + x

  def <<(x: Int): Int = value << x

  def <<(x: Long): Int = value << x

  def >>>(x: Int): Int = value >>> x

  def >>>(x: Long): Int = value >>> x

  def >>(x: Int): Int = value >> x

  def >>(x: Long): Int = value >> x

  def ==(x: Byte): Boolean = value == x

  def ==(x: Short): Boolean = value == x

  def ==(x: Char): Boolean = value == x

  def ==(x: Long): Boolean = value == x

  def ==(x: Float): Boolean = value == x

  def ==(x: Double): Boolean = value == x

  def !=(x: Byte): Boolean = value != x

  def !=(x: Short): Boolean = value != x

  def !=(x: Char): Boolean = value != x

  def !=(x: Int): Boolean = value != x

  def !=(x: Long): Boolean = value != x

  def !=(x: Float): Boolean = value != x

  def !=(x: Double): Boolean = value != x

  def <(x: Byte): Boolean = value < x

  def <(x: Short): Boolean = value < x

  def <(x: Char): Boolean = value < x

  def <(x: Int): TaintedBoolean = {
    TaintedBoolean(value < x, getProvenance(), expr < x)
  }
  def <(x: TaintedInt): TaintedBoolean = {
    TaintedBoolean(value < x.value, newProvenance(x.getProvenance()), expr < x.expr)
  }

  def <(x: Long): Boolean = value < x

  def <(x: Float): Boolean = value < x

  def <(x: Double): Boolean = value < x

  def <=(x: Byte): Boolean = value <= x

  def <=(x: Short): Boolean = value <= x

  def <=(x: Char): Boolean = value <= x

  def <=(x: Int): Boolean = value <= x
  def <=(x: TaintedInt): TaintedBoolean = {
    TaintedBoolean(value <= x.value, newProvenance(x.getProvenance()), expr <= x.expr)
  }

  def <=(x: Long): Boolean = value <= x

  def <=(x: Float): Boolean = value <= x

  def <=(x: Double): Boolean = value <= x

  def >(x: Byte): Boolean = value > x

  def >(x: Short): Boolean = value > x

  def >(x: Char): Boolean = value > x

  def >(x: Int): TaintedBoolean = {
    TaintedBoolean(value > x, getProvenance(), expr > new SymbolicInteger(x))
  }
  def >(x: TaintedInt): TaintedBoolean = {
    TaintedBoolean(value > x.value, newProvenance(x.getProvenance()), expr > x.expr)
  }

  def >(x: Long): Boolean = value > x

  def >(x: Float): Boolean = value > x

  def >(x: Double): Boolean = value > x

  def >=(x: Byte): Boolean = value >= x

  def >=(x: Short): Boolean = value >= x

  def >=(x: Char): Boolean = value >= x

  def >=(x: Int): Boolean = value >= x
  def >=(x: TaintedInt): TaintedBoolean = {
    TaintedBoolean(value >= x.value, newProvenance(x.getProvenance()), expr >= x.expr)
  }

  def >=(x: Long): Boolean = value >= x

  def >=(x: Float): Boolean = value >= x

  def >=(x: Double): Boolean = value >= x

  def |(x: Byte): Int = value | x

  def |(x: Short): Int = value | x

  def |(x: Char): Int = value | x

  def |(x: Int): Int = value | x

  def |(x: Long): Long = value | x

  def &(x: Byte): Int = value & x

  def &(x: Short): Int = value & x

  def &(x: Char): Int = value & x

  def &(x: Int): Int = value & x

  def &(x: Long): Long = value & x

  def ^(x: Byte): Int = value ^ x

  def ^(x: Short): Int = value ^ x

  def ^(x: Char): Int = value ^ x

  def ^(x: Int): Int = value ^ x

  def ^(x: Long): Long = value ^ x

  def +(x: Byte): Int = value + x

  def +(x: Short): Int = value + x

  def +(x: Char): Int = value + x

  def +(x: Long): Long = value + x

  def +(x: Float): Float = value + x

  def +(x: Double): Double = value + x

  def -(x: Byte): Int = value - x

  def -(x: Short): Int = value - x

  def -(x: Char): Int = value - x

  def -(x: Long): Long = value - x

  def -(x: Float): Float = value - x

  def -(x: Double): Double = value - x

  def *(x: Byte): Int = value * x

  def *(x: Short): Int = value * x

  def *(x: Char): Int = value * x

  def *(x: Long): Long = value * x

  def *(x: Double): Double = value * x

  def /(x: Byte): Int = value / x

  def /(x: Short): Int = value / x

  def /(x: Char): Int = value / x

  def /(x: Float): Float = value / x

  def /(x: Double): Double = value / x

  def %(x: Byte): Int = value % x

  def %(x: Short): Int = value % x

  def %(x: Char): Int = value % x

  def %(x: Long): Long = value % x

  def %(x: Float): Float = value % x

  def %(x: Double): Double = value % x

}

object TaintedInt {

  val MaxValue = new TaintedInt(Int.MaxValue)
  implicit def lift = Liftable[TaintedInt] { si =>
    q"(_root_.taintedprimitives.TaintedInt(${si.value}, ${si.p}))"
  }

  implicit def ordering: Ordering[TaintedInt] = Ordering.by(_.value)
}
