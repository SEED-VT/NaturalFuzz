package runners

import examples.{benchmarks, faulty, fuzzable, monitored, mutants,tpcds}
import fuzzer.{ProvInfo, Schema}
import org.apache.spark.util.CollectionAccumulator
import schemas.BenchmarkSchemas
import symbolicexecution.{SymExResult, SymbolicExpression}

import scala.collection.mutable.ListBuffer

object Config {

  // RIGFuzz params
  var benchmarkName = "Q1" // this value is overridden by a runner
  val keepColProb = 0.2f
  val dropMixProb = 0.5f
  val scalaVersion = 2.12
  val scoverageScalaCompilerVersion = "2.12.2"
  val maxSamples = 5
  val percentageProv = 0.1f
  val delimiter = ","
}
