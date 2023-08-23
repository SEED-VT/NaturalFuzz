package utils

import runners.Config

class SatRDDs(val rdds: Array[Seq[String]], val filterQueries: List[Query]) extends Serializable {

  def breakIntoQueryRDDs(): List[QueryResult] = {
    println(s"No. of queries ${filterQueries.length}")
    filterQueries.indices.map {
      i =>
        val mask = 0x1 << (31-i)
        val filteredRDDs = rdds.map {
          rdd =>
            rdd.filter{
              row =>
                val cols = row.split(Config.delimiter)
                val satVector = cols(cols.length - 1).toInt // get second last column (satisfaction vector)
                val sat = (satVector & mask) != 0
                if(sat) println(s"$row | satisfied | $mask")
                sat
            }
        }.map {
          rdd =>
            rdd.map{
              row =>
                val cols = row.split(Config.delimiter)
                cols.slice(0, cols.length - 1) // Get rid of last column
                  .mkString(Config.delimiter)
            }
        }

        val locs = filterQueries(i).locs
        new QueryResult(filteredRDDs, Seq(), locs)
    }.toList
  }


  def getRandMinimumSatSet(): SatRDDs = {
    new SatRDDs(rdds.map(_.sortBy(row => countBits(row.split(Config.delimiter).last.toInt))), filterQueries) // TODO: Minimize the dataset
  }

  def countBits(n: Int): Int = {
    (0 until 32).foldLeft(0){
      case (acc, i) => acc + (n >>> i) & 1
    }
  }

}
