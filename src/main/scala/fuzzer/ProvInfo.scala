package fuzzer

import org.apache.spark.rdd.RDD
import provenance.data.Provenance
import runners.Config
import taintedprimitives.Utils

import scala.collection.mutable.ListBuffer
import scala.util.Random


//depsInfo: [[(ds, col, row), (ds, col, row)], [(ds, col, row)] .... [(ds, col, row)]]
class ProvInfo(val depsInfo: ListBuffer[ListBuffer[(Int,Int,Int)]]) extends Serializable {

  val reducedDS: ListBuffer[RDD[String]] = ListBuffer()

  def update(id: Int, provenances: ListBuffer[Provenance]): Unit = {
    depsInfo.append(provenances.flatMap(_.convertToTuples))
  }

  def this() = {
    this(ListBuffer())
  }

  def getLocs(): ListBuffer[ListBuffer[(Int,Int,Int)]] = { depsInfo }

  def updateRowSet(newLocs: Map[(Int, Int), (Int, Int)]): ProvInfo = {
    new ProvInfo(depsInfo.map(
      _.map {
        case (ds, col, row) =>
          val Some((newDs, newRow)) = newLocs.get((ds, row))
          (newDs, col, newRow)
      }
    ))
  }

  def getRowLevelProvenance(): List[(Int,Int)] = {
    depsInfo.last.map{case (ds, _, row) => (ds, row)}.distinct.toList
  }

  def merge(): ProvInfo = {
    new ProvInfo(ListBuffer(depsInfo.flatten))
  }

  def append(other: ProvInfo): ProvInfo = {
    new ProvInfo(depsInfo ++ other.depsInfo)
  }

  def getRandom: ProvInfo = {
    new ProvInfo(ListBuffer(Random.shuffle(depsInfo).head))
  }

  def getCoDependentRegions: ListBuffer[ListBuffer[(Int,Int,Int)]] = { depsInfo }


  def _mergeSubsets(buffer: ListBuffer[ListBuffer[(Int, Int, Int)]]): ListBuffer[ListBuffer[(Int, Int, Int)]] = {
    buffer.foldLeft(ListBuffer[ListBuffer[(Int, Int, Int)]]()){
      case (acc, e) =>
        val keep = !buffer.filter(_.length > e.length).exists(s => e.toSet.subsetOf(s.toSet))
        if(keep) acc :+ e else acc
    }
  }

  def _mergeOverlapping(buffer: ListBuffer[ListBuffer[(Int, Int, Int)]]): ListBuffer[ListBuffer[(Int, Int, Int)]] = {
    buffer.foldLeft(ListBuffer[ListBuffer[(Int, Int, Int)]]()){
      case (acc, e) =>
        val (merged, ne) = buffer.find(s => !e.equals(s) && e.toSet.intersect(s.toSet).nonEmpty) match {
          case Some(x) => (true, (acc :+ e.toSet.union(x.toSet).to[ListBuffer]).map(_.sorted).distinct)
          case None => (false, ListBuffer(e))
        }
        if(!merged) acc :+ e else ne
    }
  }

  def _simplify(deps: ListBuffer[ListBuffer[(Int,Int,Int)]]): ListBuffer[ListBuffer[(Int,Int,Int)]] = {
    _mergeOverlapping(_mergeSubsets(deps.map(_.distinct).distinct))
  }

  def simplify(): ProvInfo = {
    new ProvInfo(_simplify(depsInfo))
  }

  override def toString: String = {
//    depsInfo.toString
    depsInfo
      .map{
        deps =>
          val row = deps.map{case (ds, row, col) => s"($ds,$row,$col)"}.mkString("<=>")
          s"$row"
      }.mkString("\n----------------------------\n")
  }
}
