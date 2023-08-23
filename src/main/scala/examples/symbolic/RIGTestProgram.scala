//package examples.symbolic
//import org.apache.spark.{SparkConf, SparkContext}
//import sparkwrapper.SparkContextWithDP
//import symbolicexecution.SymExResult
//import taintedprimitives._
//
//object RIGTestProgram extends Serializable {
//  def main(args: Array[String]): SymExResult = {
//    println(s"WebpageSegmentation args ${args.mkString(",")}")
//    val sparkConf = new SparkConf()
//    sparkConf.setMaster("local[*]")
//    sparkConf.setAppName("Webpage Segmentation").set("spark.executor.memory", "2g")
//    val testData = "mixmatch-data/rig-test/boxes"
//    val ctx = new SparkContextWithDP(new SparkContext(sparkConf))
//    ctx.setLogLevel("ERROR")
//    val testRDD = ctx.textFileProv(testData, _.split(','))
//    val testBoxes = testRDD.map(r => r.slice(1, r.length).map(_.toInt).toVector)
//    testBoxes.map(intersects).collect().foreach(println)
//    _root_.monitoring.Monitors.finalizeSymEx()
//  }
//
//  def intersects(rect1: IndexedSeq[TaintedInt]): Option[(TaintedInt, TaintedInt, TaintedInt, TaintedInt)] = {
//    val IndexedSeq(aSWx, aSWy, aHeight, aWidth) = rect1
//    println("-------")
//    println(s"RECT (${rect1.toVector})")
//    if (_root_.monitoring.Monitors.monitorPredicateSymEx(aSWx < aSWy && aHeight < aWidth, (List[Any](aSWx, aSWy, aHeight, aWidth), List[Any]()), 0)) {
//      println("if 1")
//    } else if (_root_.monitoring.Monitors.monitorPredicateSymEx(aSWx > aSWy && aHeight > aWidth, (List[Any](aSWx, aSWy, aHeight, aWidth), List[Any]()), 1)) {
//      println("if 2")
//    }
//    println("-------")
//    Some((aSWx, aSWy, aHeight, aWidth))
//  }
//}