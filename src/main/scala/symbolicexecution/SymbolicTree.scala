package symbolicexecution

import fuzzer.Schema
import org.apache.commons.lang.builder.HashCodeBuilder
import provenance.data.{DummyProvenance, Provenance}
import utils.SchemaUtils.inferType
import utils.{Query, RDDLocations}

import java.util.Objects
import scala.collection.mutable.ListBuffer

@SerialVersionUID(5L)
abstract case class SymTreeNode(s: Any) extends Serializable {

  def getValue: Any = s

  override def toString: String = s.toString
}

@SerialVersionUID(6L)
class OperationNode(override val s: Any) extends SymTreeNode(s) with Serializable {

}

@SerialVersionUID(7L)
class ConcreteValueNode(override val s: Any) extends SymTreeNode(s) with Serializable {

}


class ProvRemovedNode(override val s: Any, val prov: ListBuffer[(Int, Int, Int)], val ds: Int, val offset: Int) extends SymTreeNode(s) with Serializable {

  def modifyProv(_ds: Int, _offset: Int): ProvRemovedNode = {
    new ProvRemovedNode(s, prov, _ds, _offset)
  }

  def getProv: ListBuffer[(Int, Int, Int)] = {
    prov.map {
      case loc@(_ds, col, row) =>
        if (_ds == ds) {
          (_ds, col + offset, row)
        } else {
          loc
        }
    }
  }

  def getCol: Int = {
    getProv match {
      case null => throw new Exception("_getProv is null")
      case ListBuffer((_, col, _)) => col
      case _ => throw new Exception("Provenance is ambiguous")
    }
  }

  override def hashCode(): Int = {
    // Assuming a value only has one column as provenance
    new HashCodeBuilder()
      .append(getProv.head._1)
      .append(getProv.head._2.hashCode())
      .toHashCode
  }

  def getDS: Int = {
    getProv match {
      case null => throw new Exception("_getProv is null")
      case ListBuffer((ds, _, _)) => ds
      case _ => throw new Exception("Provenance is ambiguous")
    }
  }

  override def toString: String = getProv.map { case (ds, col, row) => s"rdd[d$ds,c$col]" }.mkString("|") //+ s"{$s}"

}

class ProvValueNode(override val s: Any, prov: Provenance, val ds: Int = 0, val offset: Int = 0) extends SymTreeNode(s) with Serializable {

  def removeProv: ProvRemovedNode = {
    new ProvRemovedNode(s, f_getProv().sorted, ds, offset)
  }

  def f_getProv(): ListBuffer[(Int, Int, Int)] = {
    prov.convertToTuples.map {
      case loc@(_ds, col, row) =>
        if (_ds == ds) {
          (_ds, col + offset, row)
        } else {
          loc
        }
    }
  }
//  override def toString: String = f_getProv.map { case (ds, col, row) => s"rdd[d$ds,c$col]" }.mkString("|") //+ s"{$s}"

}

@SerialVersionUID(4L)
case class SymbolicTree(left: SymbolicTree, node: SymTreeNode, right: SymbolicTree) extends Serializable {
  def offsetLocs(ds: Int, offset: Int): SymbolicTree = {
    val offsetNode = node match {
      case n : ProvRemovedNode =>
        n.modifyProv(ds, offset)
      case _ => node
    }

    val l = if (left != null) left.offsetLocs(ds, offset) else null
    val r = if (right != null) right.offsetLocs(ds, offset) else null

    SymbolicTree(l, offsetNode, r)
  }

  def removeProv: SymbolicTree = {
    this match {
      case SymbolicTree(null, n: ProvValueNode, null) => SymbolicTree(null, n.removeProv, null)
      case SymbolicTree(left, n: ProvValueNode, right) => SymbolicTree(left.removeProv, n.removeProv, right.removeProv)
      case SymbolicTree(null, n, null) => SymbolicTree(null, n, null)
      case node @ SymbolicTree(left, n, right) =>
//        println(s"SYMBOLIC TREE: $node")
        SymbolicTree(left.removeProv, n, right.removeProv)
    }
  }


  def this() = {
    this(null, null, null)
  }

  def this(n: SymTreeNode) = {
    this(null, n, null)
  }

  def height: Int = {
    (left, right) match {
      case (null, null) => 0
      case (null, r) => 1 + r.height
      case (l, null) => 1 + l.height
      case _ => 1 + math.max(left.height, right.height)
    }
  }

  override def hashCode(): Int = {
    val builder = new HashCodeBuilder()
      .append(node.hashCode())

    if(left != null)
      builder.append(left.hashCode())

    if(right != null)
      builder.append(right.hashCode())

    builder.toHashCode
  }

  def isOp(op: String): Boolean = {
    node.s.equals(op)
  }

  def isAtomic: Boolean = {
    !(isOp("&&") || isOp("||")) && ((left, right) match {
      case (null, null) => true
      case (null, r) => r.isAtomic
      case (l, null) => l.isAtomic
      case _ => left.isAtomic && right.isAtomic
    })
  }

  def breakIntoAtomic: List[SymbolicTree] = {
    if (isAtomic)
      List(this)
    else
      left.breakIntoAtomic ++ right.breakIntoAtomic
  }

  def isMultiDatasetQuery: Boolean = {
    new RDDLocations(getProv.toArray).isMultiDataset
  }

  def isMultiColumnQuery: Boolean = {
    false // TODO: implement this check
  }

  def eval(row: Array[String], ds: Array[Int], offsetDS2: Int = 0): Any = {

    if (node.s.toString == "contains")
      return encode(true)

    if(height == 0) {
      return node match {
        case n: ConcreteValueNode => n.s
        case n: ProvRemovedNode if ds.contains(n.getDS) =>
          // TODO: Add a callback to
          val nodeDS = n.getDS
          val col = n.getCol
          if(inferType(row(col)).dataType == Schema.TYPE_NUMERICAL) {
            try {
              row(col).toInt //TODO: Remove hardcoded type conversion to INT
            } catch {
              case _ => try {
                row(col).toFloat
              } catch {
                case _: ArrayIndexOutOfBoundsException => ""
                case _: Throwable => row(n.getCol)
              }
            }
          } else {
            try {
              row(n.getCol)
            } catch {
              case _: Throwable => ""
            }
          }
        case _ => 0
      }
    }



    val leval = left.eval(row, ds, offsetDS2)
    val reval = right.eval(row, ds, offsetDS2)


    if(leval.isInstanceOf[Boolean] || reval.isInstanceOf[Boolean]) {
      return encode(true)
    }

    (leval, reval, node.s.toString) match {
      case (l: Int, r: Int, "<") => encode(l < r)
      case (l: Int, r: Float, "<") => encode(l < r)
      case (l: Float, r: Int, "<") => encode(l < r)
      case (l: Float, r: Float, "<") => encode(l < r)

      case (l: Int, r: Int, ">") => encode(l > r)
      case (l: Int, r: Float, ">") => encode(l > r)
      case (l: Float, r: Int, ">") => encode(l > r)
      case (l: Float, r: Float, ">") => encode(l > r)

      case (l: Int, r: Int, "<=") => encode(l <= r)
      case (l: Int, r: Float, "<=") => encode(l <= r)
      case (l: Float, r: Int, "<=") => encode(l <= r)
      case (l: Float, r: Float, "<=") => encode(l <= r)

      case (l: Int, r: Int, ">=") => encode(l >= r)
      case (l: Int, r: Float, ">=") => encode(l >= r)
      case (l: Float, r: Int, ">=") => encode(l >= r)
      case (l: Float, r: Float, ">=") => encode(l >= r)

      case (l: Int, r: Int, "==") => encode(l == r)
      case (l: Int, r: Float, "==") => encode(l == r)
      case (l: Float, r: Int, "==") => encode(l == r)
      case (l: Float, r: Float, "==") => encode(l == r)

      case (l: Int, r: Int, "+") => l + r
      case (l: Int, r: Float, "+") => l + r
      case (l: Float, r: Int, "+") => l + r
      case (l: Float, r: Float, "+") => l + r

      case (l: Int, r: Int, "-") => l - r
      case (l: Int, r: Float, "-") => l - r
      case (l: Float, r: Int, "-") => l - r
      case (l: Float, r: Float, "-") => l - r

      case (l: Int, r: Int, "/") => l / r
      case (l: Int, r: Float, "/") => l / r
      case (l: Float, r: Int, "/") => l / r
      case (l: Float, r: Float, "/") => l / r

      case (l: Int, r: Int, "*") => l * r
      case (l: Int, r: Float, "*") => l * r
      case (l: Float, r: Int, "*") => l * r
      case (l: Float, r: Float, "*") => l * r

      case (l: String, r: String, "==") => encode(l == r)
      case (l: String, r: String, "!=") => encode(l != r)

      case (_:String, _, _) => encode(false)
      case (_, _, "contains") => encode(true)
      case (_, _, "nop") => 0
      case n => 0 // encode(false) // throw new Exception(s"n=$n imd=$isMultiDatasetQuery")
    }
  }

  def encode(bool: Boolean): Int = if(bool) 2 else 1

  def createFilterFn(ds: Array[Int], offsetDs2: Int = 0): Array[String] => Int = {
    node match {
      case _ if isNopTree => _ => 0
      case _ : OperationNode => row => {
//        println(s"running filter fn for $this\nrow: ${row.mkString(",")}")
        eval(row, ds, offsetDs2).asInstanceOf[Int]
      }
      case _ => throw new Exception("toQuery called on malformed tree")
    }
  }

  def isNopTree: Boolean = node.s == "nop"

  def getProv: ListBuffer[(Int, Int, Int)] = {

    (node match {
      case n: ProvRemovedNode => n.getProv
      case _ => ListBuffer()
    }) ++
      (if(left != null) left.getProv else ListBuffer()) ++
      (if(right != null) right.getProv else ListBuffer())
  }

  def toQuery: Query = {
    if(!isAtomic)
      throw new Exception("Attempted to convert non-atomic expression to query")
//    else if(isMultiDatasetQuery)
//      throw new Exception("Multi-Dataset queries not yet supported")

    def fq(rdds: Array[Seq[String]]): Array[Seq[String]] = {
      rdds
//      val filterFns = rdds.zipWithIndex.map{case (_, i) => this.createFilterFn(i)}
//      val filterdrdds = rdds.zipWithIndex.map{case (ds, i) => ds.filter(row => filterFns(i)(row.split(',')))}
//      println(s"filtered by $this")
//      filterdrdds.foreach{
//        println
//      }
//      println("filtered end")
//      filterdrdds
    }

    new Query(fq, new RDDLocations(getProv.toArray), this)
  }

  def isEmpty: Boolean = this match {
    case SymbolicTree(null, null, null) => true
    case _ => false
  }

  override def toString: String = {
    this match {
      case SymbolicTree(null, null, null) => ""
      case SymbolicTree(null, node, null) => node.toString
      case _ => s"(${left.toString} $node ${right.toString})"
    }
  }
}

object SymbolicTree {
  def nopTree(): SymbolicTree = {
    SymbolicTree(null, new OperationNode("nop"), null)
  }
}
