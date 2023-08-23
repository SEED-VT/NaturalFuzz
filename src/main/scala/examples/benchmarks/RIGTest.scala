package examples.benchmarks

import org.apache.spark.{SparkConf, SparkContext}

object RIGTest {

  def main(args: Array[String]): Unit = {
    println(s"WebpageSegmentation args ${args.mkString(",")}")
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[6]")
    sparkConf.setAppName("Webpage Segmentation").set("spark.executor.memory", "2g")
    val testData = "mixmatch-data/rig-test/boxes"
    val ctx = new SparkContext(sparkConf) //set up lineage context and start capture lineage
    ctx.setLogLevel("ERROR")
    val testRDD = ctx.textFile(testData).map(_.split(','))
    val testBoxes = testRDD.map(r => r.slice(1, r.length).map(_.toInt).toVector)
    testBoxes.map(intersects).collect().foreach(println)

  }

  def intersects(rect1: IndexedSeq[Int]): Option[(Int, Int, Int, Int)] = {

    val IndexedSeq(aSWx, aSWy, aHeight, aWidth) = rect1


    println("-------")
    println(s"RECT (${rect1.toVector})")

    if(aSWx < aSWy && aHeight < aWidth) {
      println("if 1")
    } else if(aSWx > aSWy && aHeight > aWidth) {
      println("if 2")
    }

    println("-------")
    Some((aSWx, aSWy, aHeight, aWidth))
  }

}