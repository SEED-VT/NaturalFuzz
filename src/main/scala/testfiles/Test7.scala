package testfiles

import org.apache.spark.{SparkConf,SparkContext}
import sparkwrapper.SparkContextWithDP
import scala.collection.JavaConverters._

object Test7 {

  def main(args: Array[String]): Unit = {

    val (_, sparkMaster, datasets, _, _) =
        (args(0),
          args(1),
          args.slice(args.length - 2, args.length),
          args(2),
          args(3))

    val sc = SparkContext.getOrCreate(
      new SparkConf()
        .setMaster("spark://zion-headnode:7077")
        .setAppName("Test 7")
    )
    sc.setLogLevel("ERROR")

    // create an accumulator in the driver and initialize it to an empty list
    val expressionAccumulator = sc.collectionAccumulator[String]("ExpressionAccumulator")

    val sparkConf = new SparkConf()
    sparkConf.setMaster(sparkMaster)
    sparkConf.setAppName("Test 7: Program")
    val ctx = new SparkContextWithDP(SparkContext.getOrCreate(sparkConf))

    val Array(before_data, after_data) = datasets
    val before = ctx.textFileProv(before_data, _.split(','))
    val after = ctx.textFileProv(after_data, _.split(','))

    val b = before.map {
      row =>
        if(row(0) == "www.Vm.com")
          expressionAccumulator.add(row(0).value)
        row(0)
    }

    val a = after.map {
      row =>
        if (row(0) == "www.j0.com")
          expressionAccumulator.add(row(0).value)
        row(0)
    }

    b.collect().foreach(println)
    a.collect().foreach(println)

    val exprList = expressionAccumulator.value.asScala.toList
    exprList.foreach(println)
  }

}
