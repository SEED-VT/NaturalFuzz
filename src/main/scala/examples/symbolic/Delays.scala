//package examples.symbolic
//
//import org.apache.spark.{SparkConf, SparkContext}
//import provenance.data.Provenance
//import sparkwrapper.SparkContextWithDP
//import symbolicexecution.SymExResult
//import taintedprimitives.SymImplicits._
//import taintedprimitives.TaintedInt
//import symbolicexecution.SymExResult
//
//object Delays extends Serializable {
//  def main(args: Array[String]): SymExResult = {
//    val conf = new SparkConf()
//    if (args.length < 3) throw new IllegalArgumentException("Program was called with too few args")
//    conf.setMaster(args(2))
//    conf.setAppName("symbolic.Delays")
//    val sc = new SparkContextWithDP(SparkContext.getOrCreate(conf))
//    Provenance.setProvenanceType("dual")
//    val station1 = sc.textFileProv(args(0),_.split(',')).map(r => (r(0), (r(1).toInt, r(2).toInt, r(3))))
//    val station2 = sc.textFileProv(args(1),_.split(',')).map(r => (r(0), (r(1).toInt, r(2).toInt, r(3))))
//    val joined = _root_.monitoring.Monitors.monitorJoinSymEx(station1, station2, 0)
//    val mapped = joined.map({
//      case (_, ((dep, adep, rid), (arr, aarr, _))) =>
//        (buckets((arr-aarr) - (dep-adep)), rid)
//    })
//    val grouped = _root_.monitoring.Monitors.monitorGroupByKey(mapped, 1)
//    val filtered = grouped.filter(_._1 > 2).flatMap(_._2).map((_, 1))
//    val reduced = filtered.reduceByKey(_+_)
//    reduced.take(10).foreach(println)
//
//    _root_.monitoring.Monitors.finalizeSymEx()
//  }
//  def buckets(v: TaintedInt): TaintedInt = {
//    v / 1800
//  }
//}