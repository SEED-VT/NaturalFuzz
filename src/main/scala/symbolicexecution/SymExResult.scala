package symbolicexecution

import abstraction.BaseRDD
import fuzzer.Program
import runners.Config
import utils.{Query, RDDLocations}

import scala.collection.mutable.ListBuffer

class SymExResult(val program: Program, val pathExpressions: ListBuffer[SymbolicExpression]) extends Serializable {
  def getPathQueries: List[Query] = {
    pathExpressions.flatMap(_.toCNF.toQueries).toList
  }


  def fq1a(rdds: Array[BaseRDD[String]]): Array[BaseRDD[String]] = {
    // rdds[0](1) < rdds[0](2)
    println("running fq1a")
    rdds.updated(0, rdds(0).filter{
      row =>
        val cols = row.split(Config.delimiter)
        cols(1).toInt < cols(2).toInt
    })
  }

  def fq1b(rdds: Array[BaseRDD[String]]): Array[BaseRDD[String]] = {
    // rdds[0](3) < rdds[0](4)
    println("running fq1b")
    rdds.updated(0, rdds(0).filter{
      row =>
        val cols = row.split(Config.delimiter)
        cols(3).toInt < cols(4).toInt
    })
  }

  def fq2a(rdds: Array[BaseRDD[String]]): Array[BaseRDD[String]] = {
    // rdds[0](1) > rdds[0](2)
    println("running fq2a")
    rdds.updated(0, rdds(0).filter{
      row =>
        val cols = row.split(Config.delimiter)
        cols(1).toInt > cols(2).toInt
    })
  }

  def fq2b(rdds: Array[BaseRDD[String]]): Array[BaseRDD[String]] = {
    // rdds[0](3) > rdds[0](4)
    println("running fq2b")
    rdds.updated(0, rdds(0).filter{
      row =>
        val cols = row.split(Config.delimiter)
        cols(3).toInt > cols(4).toInt
    })
  }
  /* Webpage Segmentation path conditions and filter queries

  //  PC1: rdds[0](0.concat(*).concat(5).concat(*).concat(6)) == rdds[1](0*5*6) && rdds[0](Vector(1,2,3,4)) != rdds[1](Vector(1,2,3,4)) && rdds[1](0) == rdds[1](0)
  def fq1a(rdds: Array[BaseRDD[String]]): Array[BaseRDD[String]] = {
    // rdds[0](0.concat(*).concat(5).concat(*).concat(6)) == rdds[1](0*5*6)
    println("running fq1a")
    rdds
  }

  def fq1b(rdds: Array[BaseRDD[String]]): Array[BaseRDD[String]] = {
    // rdds[0](Vector(1,2,3,4)) != rdds[1](Vector(1,2,3,4))
    rdds
  }

  def fq1c(rdds: Array[BaseRDD[String]]): Array[BaseRDD[String]] = {
    // rdds[1](0) == rdds[1](0)
    rdds
  }

  // PC2: PC1 && rdds[0](1+4) < rdds[1](1) && rdds[0](1) < rdds[1](1)
  def fq2a(rdds: Array[BaseRDD[String]]): Array[BaseRDD[String]] = {
    // rdds[0](1+4) < rdds[1](1)
    rdds
  }

  def fq2b(rdds: Array[BaseRDD[String]]): Array[BaseRDD[String]] = {
    // rdds[0](1) < rdds[1](1)
    println("running fq2b")
    rdds
  }

  // PC3: PC1 && rdds[1](1+4) < rdds[0](1) && rdds[1](1) < rdds[0](1)
  def fq3a(rdds: Array[BaseRDD[String]]): Array[BaseRDD[String]] = {
    // rdds[1](1+4) < rdds[0](1)
    rdds
  }

  def fq3b(rdds: Array[BaseRDD[String]]): Array[BaseRDD[String]] = {
    // rdds[1](1) < rdds[0](1)
    rdds
  }

  // PC4: PC1 && rdds[1](2+3) < rdds[0](2) && rdds[1](2) < rdds[0](2)
  def fq4a(rdds: Array[BaseRDD[String]]): Array[BaseRDD[String]] = {
    // rdds[1](2+3) < rdds[0](2)
    rdds
  }

  def fq4b(rdds: Array[BaseRDD[String]]): Array[BaseRDD[String]] = {
    // rdds[1](2) < rdds[0](2)
    rdds
  }

  // PC5: PC1 && rdds[0](2+3) < rdds[1](2) && rdds[0](2) < rdds[1](2)
  def fq5a(rdds: Array[BaseRDD[String]]): Array[BaseRDD[String]] = {
    // PC: rdds[0](2+3) < rdds[1](2) && rdds[0](2) < rdds[1](2)
    rdds
  }

  def fq5b(rdds: Array[BaseRDD[String]]): Array[BaseRDD[String]] = {
    // PC: rdds[0](2+3) < rdds[1](2) && rdds[0](2) < rdds[1](2)
    rdds
  }

  // PC: rdds[0](2) > rdds[1](2+3)
  def fq6(rdds: Array[BaseRDD[String]]): Array[BaseRDD[String]] = {
    // PC: rdds[0](2) > rdds[1](2+3)
    rdds
  }

  // PC: rdds[0](1) <= rdds[1](1) && rdds[1](1+4) <= rdds[0](1+4)
  def fq7a(rdds: Array[BaseRDD[String]]): Array[BaseRDD[String]] = {
    // rdds[0](1) <= rdds[1](1)
    rdds
  }

  def fq7b(rdds: Array[BaseRDD[String]]): Array[BaseRDD[String]] = {
    // rdds[1](1+4) <= rdds[0](1+4)
    rdds
  }

  // PC: rdds[1](1) <= rdds[0](1) && rdds[0](1+4) <= rdds[1](1+4)
  def fq8a(rdds: Array[BaseRDD[String]]): Array[BaseRDD[String]] = {
    // rdds[1](1) <= rdds[0](1)
    rdds
  }

  def fq8b(rdds: Array[BaseRDD[String]]): Array[BaseRDD[String]] = {
    // rdds[0](1+4) <= rdds[1](1+4)
    rdds
  }

  // PC: rdds[0](1) >= rdds[1](1) && rdds[0](1) <= rdds[1](1+4)
  def fq9a(rdds: Array[BaseRDD[String]]): Array[BaseRDD[String]] = {
    // rdds[0](1) >= rdds[1](1)
    rdds
  }

  def fq9b(rdds: Array[BaseRDD[String]]): Array[BaseRDD[String]] = {
    // rdds[0](1) <= rdds[1](1+4)
    rdds
  }

  // PC: rdds[1](1) >= rdds[0](1) && rdds[1](1) <= rdds[0](1+4)
  def fq10a(rdds: Array[BaseRDD[String]]): Array[BaseRDD[String]] = {
    // rdds[1](1) >= rdds[0](1)
    rdds
  }

  def fq10b(rdds: Array[BaseRDD[String]]): Array[BaseRDD[String]] = {
    // rdds[1](1) <= rdds[0](1+4)
    rdds
  }

  */
}
