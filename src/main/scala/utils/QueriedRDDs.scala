package utils

import org.apache.spark.rdd.RDD

class QueriedRDDs(val filteredRDDs: List[QueryResult]) extends Serializable {
  def mixMatch(inputDatasets: Array[Seq[String]]): QueryResult = {
    val inputToQR = new QueryResult(inputDatasets, Seq(), new RDDLocations(Array()))
    filteredRDDs.foldLeft(inputToQR){case (acc, e) => acc.mixMatchQueryResult(e, "random")}
  }

}
