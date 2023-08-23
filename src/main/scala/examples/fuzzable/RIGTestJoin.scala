package examples.fuzzable

import abstraction.{SparkConf, SparkContext}

object RIGTestJoin extends Serializable {

  def main(args: Array[String]): Unit = {
    println(s"RIGTestJoin: ${args.mkString(",")}")
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName("RIGTest Join")
    val ctx = new SparkContext(sparkConf)
    val ds1p = "mixmatch-data/rig-test-join/boxes1"
    val ds2p = "mixmatch-data/rig-test-join/boxes2"

    val ds1 = ctx.textFile(ds1p)
      .map(_.split(","))
      .map(row => (row(0), row.slice(1, row.length)))
    val ds2 = ctx.textFile(ds2p)
      .map(_.split(","))
      .map(row => (row(0), row.slice(1, row.length)))

    val joined = ds1.join(ds2) // PC: ds2.containsKey(ds1.col[0]) = C1

    joined.map {
      case row @ (_, (a, b)) =>
        if(a(0) > b(0)) { // PC: C1 && ds1.col[0] > ds2.col[0]
          if1()
        } else if(a(0) < b(0)) { // PC: C1 && ds1.col[0] < ds2.col[0]
          if2()
        } else if(a(0) == b(0)) { // PC: C1 && ds1.col[0] == ds2.col[0]
          if3()
        }
        row
    }
      .collect().foreach {
      case (key, (a, b)) =>
        println(key, a.mkString("-"), b.mkString("-"))
    }
  }

  def if1(): Unit = {
    println("if 1")
  }

  def if2(): Unit = {
    println("if 2")
  }

  def if3(): Unit = {
    println("if 2")
  }

}