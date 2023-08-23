package examples.symbolic

import org.apache.spark.util.CollectionAccumulator
import org.apache.spark.{SparkConf, SparkContext}
import provenance.data.Provenance
import provenance.rdd.ProvenanceRDD.toPairRDD
import runners.Config
import symbolicexecution.{SymExResult, SymbolicExpression}
import sparkwrapper.SparkContextWithDP
import taintedprimitives.SymImplicits._
import taintedprimitives.TaintedInt

object WebpageSegmentation extends Serializable {
  def main(args: Array[String], expressionAccumulator: CollectionAccumulator[SymbolicExpression]): SymExResult = {
    val sparkConf = new SparkConf()
    if (args.length < 3) throw new IllegalArgumentException("Program was called with too few args")
    sparkConf.setMaster(args(2))
    sparkConf.setAppName("symbolic.WebpageSegmentation")//.set("spark.executor.memory", "2g")
    val before_data = args(0)
    val after_data = args(1)
    val ctx = new SparkContextWithDP(SparkContext.getOrCreate(sparkConf))
    ctx.setLogLevel("ERROR")
    //    Provenance.setProvenanceType("dual")
    val before = ctx.textFileProv(before_data, _.split(','))
    val after = ctx.textFileProv(after_data, _.split(','))
    val boxes_before = before.map(r => (r(0) + "*" + r(r.length - 2) + "*" + r.last, (r(0), r.slice(1, r.length - 2).map(_.toInt).toVector)))
    val boxes_after = after.map(r => (r(0) + "*" + r(r.length - 2) + "*" + r.last, (r(0), r.slice(1, r.length - 2).map(_.toInt).toVector)))
    val boxes_after_by_site_ungrouped = after.map(r => (r(0), (r.slice(1, r.length - 2).map(_.toInt).toVector, r(r.length - 2), r.last)))
    val boxes_after_by_site = _root_.monitoring.Monitors.monitorGroupByKey(boxes_after_by_site_ungrouped, 0)

    val pairs = _root_.monitoring.Monitors.monitorJoinSymEx(boxes_before, boxes_after, 1, expressionAccumulator)
    val changed = pairs.filter({
      case (_, ((_, v1), (_, v2))) => !v1.equals(v2)
    }).map({
      case (k, ((url, b), (_, a))) =>
        val Array(_, cid, ctype) = k.split("*")
        (url, (b, cid, ctype))
    })
    val inter = _root_.monitoring.Monitors.monitorJoinSymEx(changed, boxes_after_by_site, 2, expressionAccumulator)
    inter.map {
      case (url, ((box1, _, _), lst)) =>
        (url, lst.map {
          case (box, _, _) => box
        }.map(intersects(_, box1, expressionAccumulator)))
    }.collect() //.foreach(println)
    _root_.monitoring.Monitors.finalizeSymEx(expressionAccumulator)
  }
  def intersects(rect1: IndexedSeq[TaintedInt], rect2: IndexedSeq[TaintedInt], expressionAccumulator: CollectionAccumulator[SymbolicExpression]): Option[(TaintedInt, TaintedInt, TaintedInt, TaintedInt)] = {
    val IndexedSeq(aSWx, aSWy, aHeight, aWidth) = rect1
    val IndexedSeq(bSWx, bSWy, bHeight, bWidth) = rect2
    val endpointax = aSWx + aWidth
    val startpointax = aSWx
    val endpointay = aSWy + aHeight
    val startpointay = aSWy
    val endpointbx = bSWx + bWidth
    val startpointbx = bSWx
    val endpointby = bSWy + bHeight
    val startpointby = bSWy

    if (_root_.monitoring.Monitors.monitorPredicateSymEx(endpointax < startpointbx && startpointax < startpointbx, (List[Any](endpointax, startpointbx, startpointax, startpointbx), List[Any]()), 0, expressionAccumulator)) {
      return None
    }
    if (_root_.monitoring.Monitors.monitorPredicateSymEx(endpointbx < startpointax && startpointbx < startpointax, (List[Any](endpointbx, startpointax, startpointbx, startpointax), List[Any]()), 1, expressionAccumulator)) {
      return None
    }
    if (_root_.monitoring.Monitors.monitorPredicateSymEx(endpointby < startpointay && startpointby < startpointay, (List[Any](endpointby, startpointay, startpointby, startpointay), List[Any]()), 2, expressionAccumulator)) {
      return None
    }
    if (_root_.monitoring.Monitors.monitorPredicateSymEx(endpointay < startpointby && startpointay < startpointby, (List[Any](endpointay, startpointby, startpointay, startpointby), List[Any]()), 3, expressionAccumulator)) {
      return None
    }
    if (_root_.monitoring.Monitors.monitorPredicateSymEx(startpointay > endpointby, (List[Any](startpointay, endpointby), List[Any]()), 4, expressionAccumulator)) {
      return None
    }
    var iSWx, iSWy, iWidth, iHeight = new TaintedInt(0)

    if (_root_.monitoring.Monitors.monitorPredicateSymEx(startpointax <= startpointbx && endpointbx <= endpointax, (List[Any](startpointax, startpointbx, endpointbx, endpointax), List[Any]()), 5, expressionAccumulator)) {
      iSWx = startpointbx
      iSWy = if (_root_.monitoring.Monitors.monitorPredicateSymEx(startpointay < startpointby, (List[Any](startpointay, startpointby), List[Any](startpointax, startpointbx, endpointbx, endpointax)), 6, expressionAccumulator)) startpointby else startpointay
      iWidth = bWidth
      val top = if (_root_.monitoring.Monitors.monitorPredicateSymEx(endpointby < endpointay, (List[Any](endpointby, endpointay), List[Any](startpointax, startpointbx, endpointbx, endpointax)), 7, expressionAccumulator)) endpointby else endpointay
      iHeight = top - iSWy
    } else if (_root_.monitoring.Monitors.monitorPredicateSymEx(startpointbx <= startpointax && endpointax <= endpointbx, (List[Any](startpointbx, startpointax, endpointax, endpointbx), List[Any](startpointax, startpointbx, endpointbx, endpointax)), 8, expressionAccumulator)) {
      iSWx = startpointax
      iSWy = if (_root_.monitoring.Monitors.monitorPredicateSymEx(startpointay < startpointby, (List[Any](startpointay, startpointby), List[Any](startpointax, startpointbx, endpointbx, endpointax, startpointbx, startpointax, endpointax, endpointbx)), 9, expressionAccumulator)) startpointby else startpointay
      iWidth = aWidth
      val top = if (_root_.monitoring.Monitors.monitorPredicateSymEx(endpointby < endpointay, (List[Any](endpointby, endpointay), List[Any](startpointax, startpointbx, endpointbx, endpointax, startpointbx, startpointax, endpointax, endpointbx)), 10, expressionAccumulator)) endpointby else endpointay
      iHeight = top - iSWy
    } else if (_root_.monitoring.Monitors.monitorPredicateSymEx(startpointax >= startpointbx && startpointax <= endpointbx, (List[Any](startpointax, startpointbx, startpointax, endpointbx), List[Any](startpointax, startpointbx, endpointbx, endpointax, startpointbx, startpointax, endpointax, endpointbx)), 11, expressionAccumulator)) {
      iSWx = startpointax
      iSWy = if (_root_.monitoring.Monitors.monitorPredicateSymEx(startpointay > startpointby, (List[Any](startpointay, startpointby), List[Any](startpointax, startpointbx, endpointbx, endpointax, startpointbx, startpointax, endpointax, endpointbx, startpointax, startpointbx, startpointax, endpointbx)), 12, expressionAccumulator)) startpointay else startpointby
      iWidth = endpointbx - startpointax
      val top = if (_root_.monitoring.Monitors.monitorPredicateSymEx(endpointby < endpointay, (List[Any](endpointby, endpointay), List[Any](startpointax, startpointbx, endpointbx, endpointax, startpointbx, startpointax, endpointax, endpointbx, startpointax, startpointbx, startpointax, endpointbx)), 13, expressionAccumulator)) endpointby else endpointay
      iHeight = top - iSWy
    } else if (_root_.monitoring.Monitors.monitorPredicateSymEx(startpointbx >= startpointax && startpointbx <= endpointax, (List[Any](startpointbx, startpointax, startpointbx, endpointax), List[Any](startpointax, startpointbx, endpointbx, endpointax, startpointbx, startpointax, endpointax, endpointbx, startpointax, startpointbx, startpointax, endpointbx)), 14, expressionAccumulator)) {
      iSWx = startpointbx
      iSWy = if (_root_.monitoring.Monitors.monitorPredicateSymEx(startpointay > startpointby, (List[Any](startpointay, startpointby), List[Any](startpointax, startpointbx, endpointbx, endpointax, startpointbx, startpointax, endpointax, endpointbx, startpointax, startpointbx, startpointax, endpointbx, startpointbx, startpointax, startpointbx, endpointax)), 15, expressionAccumulator)) startpointay else startpointby
      iWidth = endpointax - startpointbx
      val top = if (_root_.monitoring.Monitors.monitorPredicateSymEx(endpointby < endpointay, (List[Any](endpointby, endpointay), List[Any](startpointax, startpointbx, endpointbx, endpointax, startpointbx, startpointax, endpointax, endpointbx, startpointax, startpointbx, startpointax, endpointbx, startpointbx, startpointax, startpointbx, endpointax)), 16, expressionAccumulator)) endpointby else endpointay
      iHeight = top - iSWy
    }
    else if (_root_.monitoring.Monitors.monitorPredicateSymEx(startpointbx > startpointax && startpointbx < endpointax && endpointby <= startpointay, (List[Any](startpointbx, startpointax, startpointbx, endpointax, endpointby, startpointay), List[Any](startpointax, startpointbx, endpointbx, endpointax, startpointbx, startpointax, endpointax, endpointbx, startpointax, startpointbx, startpointax, endpointbx, startpointbx, startpointax, startpointbx, endpointax)), 17, expressionAccumulator)) {
      iSWx = startpointbx
      iSWy = startpointay
      iWidth = bWidth
      iHeight = 0
    } else if (_root_.monitoring.Monitors.monitorPredicateSymEx(startpointax > startpointbx && startpointax < endpointbx && endpointay <= startpointby, (List[Any](startpointax, startpointbx, startpointax, endpointbx, endpointay, startpointby), List[Any](startpointax, startpointbx, endpointbx, endpointax, startpointbx, startpointax, endpointax, endpointbx, startpointax, startpointbx, startpointax, endpointbx, startpointbx, startpointax, startpointbx, endpointax, startpointbx, startpointax, startpointbx, endpointax, endpointby, startpointay)), 18, expressionAccumulator)) {
      iSWx = startpointax
      iSWy = endpointby
      iWidth = aWidth
      iHeight = 0
    }
    Some((iSWx, iSWy, iHeight, iWidth))
  }
}