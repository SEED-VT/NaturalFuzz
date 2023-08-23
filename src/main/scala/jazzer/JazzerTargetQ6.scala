package jazzer

import com.code_intelligence.jazzer.api.FuzzedDataProvider

object JazzerTargetQ6 {

  var mode: String = ""
  var pkg: String = ""
  var measurementsDir: String = ""
  var mutantName = ""
  val datasets: Array[String] = Array(
    "dataset_0",
    "dataset_1",
    "dataset_2",
    "dataset_3",
    "dataset_4"
  )
  var f: Array[String] => Unit = null
  var f_mutant: Array[String] => Unit = null

  def fuzzerInitialize(args: Array[String]): Unit = {
    measurementsDir = args(0)
    mode = args(1)
    pkg = args(2)
    mutantName = args(3)

    f = pkg match {
      case "faulty" => examples.faulty.Q6.main
    }

    f_mutant = mutantName.takeRight(2) match {
      case "M0" => examples.mutants.Q6.Q6_M0.main
      case "M1" => examples.mutants.Q6.Q6_M1.main
      case "M2" => examples.mutants.Q6.Q6_M2.main
      case "M3" => examples.mutants.Q6.Q6_M3.main
      case "M4" => examples.mutants.Q6.Q6_M4.main
      case "M5" => examples.mutants.Q6.Q6_M5.main
      case "M6" => examples.mutants.Q6.Q6_M6.main
      case "M7" => examples.mutants.Q6.Q6_M7.main
      case "M8" => examples.mutants.Q6.Q6_M8.main
      case "M9" => examples.mutants.Q6.Q6_M9.main
      case "M10" => examples.mutants.Q6.Q6_M10.main
      case "M11" => examples.mutants.Q6.Q6_M11.main
      case "M12" => examples.mutants.Q6.Q6_M12.main
    }

    SharedJazzerLogic.fuzzerInitialize(args,f,f_mutant)
  }

  def fuzzerTestOneInput(data: FuzzedDataProvider): Unit = {
    // Might need to manipulate scoverage measurement files produced by execution
    // since the old one will be overridden (P.S. not true) on next call or to indicate sequence
    // maybe attach iteration number to it

    // Schema ds1 & ds2: string,int,int,int,int,int,string


    mode match {
      case "reproduce" => SharedJazzerLogic.fuzzTestOneInput(
        data,
        datasets,
        f
      )
      case "fuzz" => SharedJazzerLogic.fuzzTestOneInput(
        data,
        f,
        measurementsDir,
        datasets
      )
      case "mutant" => SharedJazzerLogic.fuzzTestOneInputMutant(
        data,
        f,
        f_mutant,
        measurementsDir,
        datasets
      )
    }
  }

}
