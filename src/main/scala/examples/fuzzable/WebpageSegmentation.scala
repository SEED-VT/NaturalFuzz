package examples.fuzzable

import abstraction.{SparkConf, SparkContext}

object WebpageSegmentation {

  def main(args: Array[String]): Unit = {
    println(s"WebpageSegmentation args ${args.mkString(",")}")
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[6]")
    sparkConf.setAppName("Webpage Segmentation").set("spark.executor.memory", "2g")
    val before_data = args(0) // "seeds/weak_seed/webpage_segmentation/before"
    val after_data = args(1) // "seeds/weak_seed/webpage_segmentation/after"
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
      .map{
        case (k, ((_, a), (url, _))) =>
          val Array(_, cid, ctype) = k.split('*')
          (url, (a, cid, ctype))
      }

    println("changed")
    changed.collect().foreach(println)

    println("boxes_after_by_site")
    boxes_after_by_site.collect().foreach(println)


    val inter = changed.join(boxes_after_by_site)
    println("inter")
    inter.collect().foreach(println)
    inter.map{
      case (url, ((box1, _, _), lst)) => (url, lst.map{case (box, _, _) => box}.map(box2 => intersects(box1, box2)))
    }.collect().foreach(println)

  }

  def intersects(rect1: IndexedSeq[Int],
                 rect2: IndexedSeq[Int]): Option[(Int, Int, Int, Int)] = {

    val IndexedSeq(aSWx, aSWy, aHeight, aWidth) = rect1 // bug: last column missing in both datasets will trigger match error
    val IndexedSeq(bSWx, bSWy, bHeight, bWidth) = rect2 // bug
    val endpointax = aSWx + aWidth;
    val startpointax = aSWx;
    val endpointay = aSWy + aHeight;
    val startpointay = aSWy;
    val endpointbx = bSWx + bWidth;
    val startpointbx = bSWx;
    val endpointby = bSWy + bHeight;
    val startpointby = bSWy;


    println("-------")
    println(s"A(${rect1.toVector}) B(${rect2.toVector})")

    if ((endpointax < startpointbx) && (startpointax < startpointbx) ){
      return None
    }
    if ((endpointbx < startpointax) && (startpointbx < startpointax)){
      return None
    }

    if ((endpointby < startpointay) && (startpointby < startpointay)){
      return None
    }

    if ((endpointay < startpointby) && (startpointay < startpointby)){
      return None
    }

    if (startpointay > endpointby){
      return None
    }

    var iSWx, iSWy, iWidth, iHeight  = 0

    if ((startpointax <= startpointbx) && (endpointbx <= endpointax)) {
      println("if 1")
      iSWx  = startpointbx
      iSWy = if (startpointay < startpointby) startpointby else startpointay
      iWidth = bWidth
      val top = if (endpointby < endpointay) endpointby else endpointay
      iHeight = top - iSWy
    }
    else if ((startpointbx <= startpointax) && (endpointax <= endpointbx)) {
      println("if 2")
      iSWx  = startpointax
      iSWy = if (startpointay < startpointby) startpointby else startpointay
      iWidth = aWidth
      val top = if (endpointby < endpointay) endpointby  else endpointay
      iHeight = top - iSWy
    }
    else if ((startpointax >= startpointbx) && (startpointax <= endpointbx)) {
      println("if 3")
      iSWx  = startpointax
      iSWy = if (startpointay > startpointby) startpointay else startpointby
      iWidth = endpointbx - startpointax
      val top = if (endpointby < endpointay) endpointby  else endpointay
      iHeight = top - iSWy
    }
    else if ((startpointbx >= startpointax) && (startpointbx <= endpointax)) {
      println("if 4")
      iSWx  = startpointbx
      iSWy = if (startpointay > startpointby) startpointay else startpointby
      iWidth = endpointax - startpointbx
      val top = if (endpointby < endpointay) endpointby  else endpointay
      iHeight = top - iSWy
    }

    println("-------")
    Some((iSWx, iSWy, iHeight, iWidth))
  }

}