package examples.benchmarks

import org.apache.spark.{SparkConf, SparkContext}

object WebpageSegmentation extends Serializable {

  def main(args: Array[String]): Unit = {
    println(s"webpage WebpageSegmentation args ${args.mkString(",")}")
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName("Webpage Segmentation").set("spark.executor.memory", "2g")
    val before_data = args(0) // "datasets/fuzzing_seeds/webpage_segmentation/before"
    val after_data = args(1) // "datasets/fuzzing_seeds/webpage_segmentation/after"
    val ctx = new SparkContext(sparkConf) //set up lineage context and start capture lineage
    ctx.setLogLevel("ERROR")
    val before = ctx.textFile(before_data).map(_.split(','))
    val after = ctx.textFile(after_data).map(_.split(','))

    val boxes_before = before.map(r => (s"${r(0)}*${r(r.length-2)}*${r.last}", (r(0), r.slice(1, r.length-2).map(_.toInt).toVector)))
    val boxes_after = after.map(r => (s"${r(0)}*${r(r.length-2)}*${r.last}", (r(0), r.slice(1, r.length-2).map(_.toInt).toVector)))
    val boxes_after_by_site = after.map(r => (r(0), (r.slice(1, r.length-2).map(_.toInt).toVector, r(r.length-2), r.last)))
      .groupByKey()
    //    after.collect().foreach(r => println(r.toVector))

    val pairs = boxes_before.join(boxes_after)
    val changed = pairs
      .filter{
        case (_, ((_, v1), (_, v2))) => !v1.equals(v2)
      }
      .map {
        case (k, (_, (url, a))) =>
          val Array(_, cid, ctype) = k.split('*')
          (url, (a, cid, ctype))
      }

//    println("-------")
    val inter = changed.join(boxes_after_by_site)
    inter.map{
      case (url, ((box1, _, _), lst)) => (url, lst.map{case (box, _, _) => box}.map(intersects(_, box1)))
    }.collect().foreach(println)
    //    val iRects =  pairs.map{ case (id, (rect1, rect2)) => (id, intersects(rect1, rect2))}
    //    iRects.collect().foreach(println)
//    ctx.stop()
  }

  def intersects(rect1: IndexedSeq[Int],
                 rect2: IndexedSeq[Int]): Option[(Int, Int, Int, Int)] = {
//  Path condition for reaching this function
//  boxes_before.row(x).col(0) == boxes_after.row(y).col(0) && boxes_before.row(x).col(1) != boxes_after.row(y).col(1) && after.row(0) == after(0)
    val IndexedSeq(aSWx, aSWy, aHeight, aWidth) = rect1
    val IndexedSeq(bSWx, bSWy, bHeight, bWidth) = rect2
    val endpointax = aSWx + aWidth // before(1+4)
    val startpointax = aSWx // before(1)
    val endpointay = aSWy + aHeight // before(2+3)
    val startpointay = aSWy // before(2)
    val endpointbx = bSWx + bWidth // after(1+4)
    val startpointbx = bSWx // after(1)
    val endpointby = bSWy + bHeight // after(2+3)
    val startpointby = bSWy // after(2)


    // PC: before(1+4) < after(1) && before(1) < after(1)
    if ((endpointax < startpointbx) && (startpointax < startpointbx) ){
      return None
    }
    // PC: after(1+4) < before(1) && after(1) < before(1)
    if ((endpointbx < startpointax) && (startpointbx < startpointax)){
      return None
    }
    // PC: after(2+3) < before(2) && after(2) < before(2)
    if ((endpointby < startpointay) && (startpointby < startpointay)){
      return None
    }
    // PC: before(2+3) < after(2) && before(2) < after(2)
    if ((endpointay < startpointby) && (startpointay < startpointby)){
      return None
    }
    // PC: before(2) > after(2+3)
    if (startpointay > endpointby){
      return None
    }

    var iSWx, iSWy, iWidth, iHeight  = 0

    // PC: before(1) <= after(1) && after(1+4) <= before(1+4)
    if ((startpointax <= startpointbx) && (endpointbx <= endpointax)) {
      iSWx  = startpointbx
      iSWy = if (startpointay < startpointby) startpointby else startpointay
      iWidth = bWidth
      val top = if (endpointby < endpointay) endpointby else endpointay
      iHeight = (top - iSWy)
    }
    // PC: after(1) <= before(1) && before(1+4) <= after(1+4)
    else if ((startpointbx <= startpointax) && (endpointax <= endpointbx)) {
      iSWx  = startpointax
      iSWy = if (startpointay < startpointby) startpointby else startpointay
      iWidth = aWidth
      val top = if (endpointby < endpointay) endpointby  else endpointay
      iHeight = (top - iSWy)
    }
    // PC: before(1) >= after(1) && before(1) <= after(1+4)
    else if ((startpointax >= startpointbx) && (startpointax <= endpointbx)) {
      iSWx  = startpointax
      iSWy = if (startpointay > startpointby) startpointay else startpointby
      iWidth = (endpointbx - startpointax)
      val top = if (endpointby < endpointay) endpointby  else endpointay
      iHeight = (top - iSWy)
    }

    // PC: after(1) >= before(1) && after(1) <= before(1+4)
    else if ((startpointbx >= startpointax) && (startpointbx <= endpointax)) {
      iSWx  = startpointbx
      iSWy = if (startpointay > startpointby) startpointay else startpointby
      iWidth = (endpointax - startpointbx)
      val top = if (endpointby < endpointay) endpointby  else endpointay
      iHeight = (top - iSWy)
    }

    Some(iSWx, iSWy, iHeight, iWidth)
  }

}