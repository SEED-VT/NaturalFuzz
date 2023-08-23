package utils

import abstraction.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import runners.Config
import symbolicexecution.SymExResult
import utils.MiscUtils.toBinaryStringWithLeadingZeros

class FilterQueries(val symExResult: SymExResult) extends Serializable {

  val filterQueries: List[Query] = symExResult.getPathQueries.distinct

  def getCount: Int = scala.math.min(filterQueries.length, 16)
  def filterPiecesForDS(i: Int): List[Query] = {
    filterQueries.filter(q => q.locs.involvesDS(i))
  }

  def getJoinConditions: List[(Int, Int, List[Int], List[Int])] = {
    filterQueries
      .filter(_.tree.node.s == "contains")
      .filter(_.isMultiDatasetQuery)
      .map {q =>
        q.locs.to4Tuple
      }
//    List((0, 1, List(0,5,6), List(0,5,6))) // get
  }

//  def getRows(datasets: Array[String]): List[QueryResult] = {
//    val sc = new SparkContext(null)
//    val rdds = datasets.map(sc.textFile)
//    filterQueries.map(fq => fq.runQuery(rdds))
//  }

  def createSatVectors(rdds: Array[RDD[String]]): Array[RDD[(String, Int)]] = {
    if (filterQueries.length > 32 / 2)
      println("Too many conditions, unable to encode as 32 bit integer. Skipping some.")

    val filterQueries16 = filterQueries.take(16)

    val satRDD = rdds
      .zipWithIndex
      .map {
        case (rdd, i) => {
          rdd.map {
            row =>
              val filterQueriesDS = filterQueries16
                .map {
                  q =>
                    if (q.involvesDS(i)) q else Query.dummyQuery()
                }
              val filterFns = filterQueriesDS
                .map(_.tree.createFilterFn(Array(i)))
              val cols = row.split(Config.delimiter)
              val sat = makeSatVector(filterFns.zip(filterQueriesDS), cols)
              (cols.mkString(Config.delimiter), sat)
          }
        }
      }
    satRDD
  }

  def createSatVectors(
                        rdds: Array[RDD[((String, Int), Long)]],
                        savedJoins: Array[(RDD[(String, ((String, Long), (String, Long)))], Int, Int)]
                      ): Array[RDD[((String, Int), Long)]] =
  {
    if (filterQueries.length > 32 / 2)
      println("Too many conditions, unable to encode as 32 bit integer. Skipping some.")

    val filterQueries16 = filterQueries.take(16)

    val savedJoinsWithPV = savedJoins.map { // j is the joined dataset, a and b are dataset ids of joined datasets
      case (rdd, dsA, dsB) =>
        val filterQueriesDS = filterQueries16
          .map { // create a list of only multi dataset queries involving dsA and dsB
            // the reason filter is not used is because we need to make sure the bit vector is filled at the right locations
            q =>
              if (q.involvesDS(dsA) && q.involvesDS(dsB)) {
                q
              } else {
                Query.dummyQuery() // this will result in a 00 at the location
              }
          }
        val ds2Offset = rdd.take(1)(0)._2._1._1.split(Config.delimiter).length
        val filterFns = filterQueriesDS
          .map { q => q.offsetLocs(dsB, ds2Offset) }
          .map(_.tree.createFilterFn(Array(dsA, dsB), ds2Offset))
        (rdd.map {
          case (k, ((rowA, rowAi), (rowB, rowBi))) =>
            val combinedRow = s"$rowA${Config.delimiter}$rowB"
            val pv = makeSatVector(filterFns.zip(filterQueriesDS), combinedRow.split(Config.delimiter), true)
//            println(s"jrow: $combinedRow - ${toBinaryStringWithLeadingZeros(pv)}")
            (k, ((rowA, rowAi, pv), (rowB, rowBi, pv)))
        }, dsA, dsB)
    }

    val transformed = savedJoinsWithPV.flatMap {
      case (rdd, dsA, dsB) =>
        Array(
          (rdd.map {
            case (_, ((rowA, rowAi, pv1), _)) =>
              ((dsA,rowAi), (rowA, pv1))
          }.distinct, dsA),
          (rdd.map {
            case (_, (_, (rowB, rowBi, pv2))) =>
              ((dsB,rowBi), (rowB, pv2))
          }.distinct, dsB)
        )
    }

    val filtered = rdds
      .indices
      .map {
        i =>
          transformed.filter { case (_, ds) => i == ds}
      }

    val transformedInputRDDs = rdds
      .zipWithIndex
      .map {
        case (rdd, ds) =>
          rdd.map {
            case (row, rowi) =>
              ((ds, rowi), row)
          }
      }

    val finalPathVectors = transformedInputRDDs
      .zipWithIndex
      .map {
        case (rdd, i) =>
          filtered(i)
            .foldLeft(rdd) {
              case (acc, (e, _)) =>
                acc
                  .leftOuterJoin(e)
                  .map {
                    case (key, ((row, pv1), Some((_, pv2)))) =>
                      (key, (row, pv1 | pv2))
                    case (key, (left, _)) =>
                      (key, left)
                  }
            }
      }

    println("\n BEGIN PRINTING JOINS\n ")
    finalPathVectors
      .zipWithIndex
      .foreach {
        case (joined, i) =>
          println(s"JOINED $i")
          joined
            .take(10)
            .foreach(println)
      }
    println("\n END PRINTING JOINS \n")

    // TODO: Final reduceByKey to merge duplicated rows after joins

    finalPathVectors
      .map {
        rdd =>
          rdd.map {
            case ((_, rowi), (row, pv)) =>
              ((row, pv), rowi)
          }
      }
//    val savedJoinsWithPV = savedJoins.map { // j is the joined dataset, a and b are dataset ids of joined datasets
//      case (rdd, dsA, dsB) =>
//        (rdd.map {
//          case (k, ((rowA, rowAi), (rowB, rowBi))) =>
//            val combinedRow = s"$rowA,$rowB"
//            val filterFns = filterQueries
//              .filter { q => q.involvesDS(dsA) && q.involvesDS(dsB) }
//              .map(_.tree.createFilterFn(Array(dsA, dsB)))
//            val pv = makeSatVector(filterFns, combinedRow.split(","))
//            (k, ((rowA, rowAi, pv), (rowB, rowBi, pv)))
//        }, dsA, dsB)
//    }
//
//    rdds.zipWithIndex.map {
//      case (rdd, dsi) =>
//        rdd.zipWithIndex.map {
//          case (row, rowi) =>
//            val newPathVector = combinePathVectorsForRow((dsi, rowi), savedJoinsWithPV)
//            modifyPathVector(row, newPathVector)
//        }
//    }
  }

  def combinePathVectorsForRow(loc: (Int, Long), savedJoinsWithPVs: Array[(RDD[(String, ((String, Long, Int), (String, Long, Int)))], Int, Int)]): Int = {
    val (ds, row) = loc
    val involvedDS = savedJoinsWithPVs
      .filter {
        case (_, dsA, dsB) =>
          dsA == ds || dsB == ds
      }
      .map {
        case (rdd, dsA, _) =>
          rdd.map {
            case (_, (rowDSA, rowDSB)) =>
              if(ds == dsA) rowDSA else rowDSB
          }
      }

    involvedDS.foldLeft(0) {
      case (acc, rdd) =>
        acc | rdd.aggregate(0)({
          case (acc, (_, rowi, pv)) =>
            if (rowi == row) acc | pv else acc
        }, {
          case (pvA, pvB) => pvA | pvB
        })
    }
  }

  def makeSatVector(conditions: List[(Array[String] => Int, Query)], row: Array[String], combined: Boolean = false): Int = {

    val pv = conditions
      .map {
        case (f, q) =>
          val check = !combined && q.isMultiDatasetQuery
          if (check)
            0x00
          else {
            val ret = f(row)
            ret
          }
      }
      .zipWithIndex
      .foldLeft(0) {
        case (acc, (b, i)) =>
          acc | (b << (30 - i * 2))
      }

//    val constr = conditions.map(_._2.tree).mkString("\n")
    val pvstr = toBinaryStringWithLeadingZeros(pv).take(conditions.length*2)
    pv
  }

}
