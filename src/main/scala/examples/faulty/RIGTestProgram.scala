package examples.faulty

import abstraction.{SparkConf, SparkContext}

object RIGTestProgram {

  def main(args: Array[String]): Unit = {
    println(s"RIGTestProgram args: ${args.mkString(",")}")
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName("RIGTestProgram")
    val testData = args(0) // "seeds/weak_seed/webpage_segmentation/before"
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