//package examples.clustermonitor
//
//import fuzzer.ProvInfo
//import org.apache.spark.{SparkConf, SparkContext}
//import sparkwrapper.SparkContextWithDP
//import symbolicexecution.SymExResult
//import taintedprimitives._
//
//object RIGTestProgram extends Serializable {
//  def main(args: Array[String]): ProvInfo = {
//    println(s"RIGTestProgram args: ${args.mkString(",")}")
//    val sparkConf = new SparkConf()
//    if (args.length < 2) throw new IllegalArgumentException("Program was called with too few args")
//    sparkConf.setMaster(args(1))
//    sparkConf.setAppName("RIGTestProgram")
//    val testData = args(0)
//    val ctx = new SparkContextWithDP(new SparkContext(sparkConf))
//    ctx.setLogLevel("ERROR")
//    val testRDD = ctx.textFileProv(testData, _.split(','))
//    val testBoxes = testRDD.map(r => r.slice(1, r.length).map(_.toInt).toVector)
//    testBoxes.map(intersects).collect().foreach(println)
//    _root_.monitoring.Monitors.finalizeProvenance()
//  }
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