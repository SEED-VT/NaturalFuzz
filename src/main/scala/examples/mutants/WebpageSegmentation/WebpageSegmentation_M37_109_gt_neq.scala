package examples.mutants.WebpageSegmentation
import abstraction.{ SparkConf, SparkContext }
import capture.IOStreams._println
object WebpageSegmentation_M37_109_gt_neq {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local[*]")
    sparkConf.setAppName("Webpage Segmentation")
    val before_data = args(0)
    val after_data = args(1)
    val ctx = new SparkContext(sparkConf)
    ctx.setLogLevel("ERROR")
    val before = ctx.textFile(before_data).map(_.split(','))
    val after = ctx.textFile(after_data).map(_.split(','))
    val boxes_before = before.map(r => (s"${r(0)}*${r(r.length - 2)}*${r.last}", (r(0), r.slice(1, r.length - 2).map(_.toInt).toVector)))
    val boxes_after = after.map(r => (s"${r(0)}*${r(r.length - 2)}*${r.last}", (r(0), r.slice(1, r.length - 2).map(_.toInt).toVector)))
    val boxes_after_by_site = after.map(r => (r(0), (r.slice(1, r.length - 2).map(_.toInt).toVector, r(r.length - 2), r.last))).groupByKey()
    val pairs = boxes_before.join(boxes_after)
    val changed = pairs.filter({
      case (_, ((_, v1), (_, v2))) =>
        !v1.equals(v2)
    }).map({
      case (k, (_, (url, a))) =>
        val Array(_, cid, ctype) = k.split('*')
        (url, (a, cid, ctype))
    })
    val inter = changed.join(boxes_after_by_site)
    inter.map({
      case (url, ((box1, _, _), lst)) =>
        (url, lst.map({
          case (box, _, _) => box
        }).map(intersects(_, box1)))
    }).collect().foreach(_println)
  }
  def intersects(rect1: IndexedSeq[Int], rect2: IndexedSeq[Int]): Option[(Int, Int, Int, Int)] = {
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
    if (endpointax < startpointbx && startpointax < startpointbx) {
      return None
    }
    if (endpointbx < startpointax && startpointbx < startpointax) {
      return None
    }
    if (endpointby < startpointay && startpointby < startpointay) {
      return None
    }
    if (endpointay < startpointby && startpointay < startpointby) {
      return None
    }
    if (startpointay > endpointby) {
      return None
    }
    var iSWx, iSWy, iWidth, iHeight = 0
    if (startpointax <= startpointbx && endpointbx <= endpointax) {
      iSWx = startpointbx
      iSWy = if (startpointay < startpointby) startpointby else startpointay
      iWidth = bWidth
      val top = if (endpointby < endpointay) endpointby else endpointay
      iHeight = top - iSWy
    } else if (startpointbx <= startpointax && endpointax <= endpointbx) {
      iSWx = startpointax
      iSWy = if (startpointay < startpointby) startpointby else startpointay
      iWidth = aWidth
      val top = if (endpointby < endpointay) endpointby else endpointay
      iHeight = top - iSWy
    } else if (startpointax >= startpointbx && startpointax <= endpointbx) {
      iSWx = startpointax
      iSWy = if (startpointay > startpointby) startpointay else startpointby
      iWidth = endpointbx - startpointax
      val top = if (endpointby < endpointay) endpointby else endpointay
      iHeight = top - iSWy
    } else if (startpointbx >= startpointax && startpointbx <= endpointax) {
      iSWx = startpointbx
      iSWy = if (startpointay != startpointby) startpointay else startpointby
      iWidth = endpointax - startpointbx
      val top = if (endpointby < endpointay) endpointby else endpointay
      iHeight = top - iSWy
    }
    Some((iSWx, iSWy, iHeight, iWidth))
  }
}