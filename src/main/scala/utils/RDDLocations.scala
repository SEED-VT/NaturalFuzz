package utils

@SerialVersionUID(3L)
class RDDLocations(val locs: Array[(Int, Int, Int)]) extends Serializable {
  def offsetLocs(ds: Int, offset: Int): RDDLocations = {
    new RDDLocations(locs.map {
      case loc @ (_ds, col, row) =>
        if (ds == _ds) {
          (ds, col+offset,row)
        } else {
          loc
        }
    })
  }


  val dsCols: Map[(Int, Int), Array[Int]] = locs
    .groupBy {case (ds, col, _) => (ds, col)}
    .mapValues(arr => arr.map{case (_, _, r) => r})
    .map(identity)

  def to4Tuple: (Int, Int, List[Int], List[Int]) = {
    val inter = locs
      .map { case (ds, col, _) => (ds, col) }
      .distinct
      .groupBy { case (ds, _) => ds }
      .map {
        case (ds, arr) => (ds, arr.map(_._2).toList)
      }

    (inter.keys.toList(0), inter.keys.toList(1), inter.values.toList(0).sorted, inter.values.toList(1).sorted)
  }

  def getCols(dsi: Int): Array[Int] = {
    dsCols
      .filter{case ((ds, _), _) => dsi == ds}
      .map{case ((_, col), _) => col}
      .toArray
  }

  def involvesDS(ds: Int): Boolean = {
    locs.exists {case (d, _, _) => d == ds}
  }

  override def toString: String = locs.map{case (ds,col,row) => s"($ds,$col,$row)"}.mkString("|")

  def isMultiDataset: Boolean = {
    locs.map { case (ds, _, _) => ds}.distinct.length > 1
  }
}
