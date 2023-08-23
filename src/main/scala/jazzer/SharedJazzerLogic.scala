package jazzer

import com.code_intelligence.jazzer.api.FuzzedDataProvider
import scoverage.Platform.FileWriter
import scoverage.Serializer
import utils.{FileUtils, IOUtils}
import fuzzer.NewFuzzer._
import fuzzer._
import java.io.{File, FileFilter}
import scala.reflect.io.Directory

object SharedJazzerLogic {

  var prevCov = 0.0
  var t_start: Long = 0
  var lastCoverage = 0.0;
  var testCaseOutDir = ""
  var coverageOutDir = ""
  var refCoverageOutDir = ""
  var mutantKilled = false
  var postMutantKill = false
  var refExecStats: ExecStats = null
  var mutantExecStats: ExecStats = null
  var refProgram: ExecutableProgram = null
  var mutantProgram: ExecutableProgram = null
  var stats = new FuzzStats("ref")
  var outDir = ""

  def updateIteration(measurementsDir: String): Unit = {
    new FileWriter(new File(s"$measurementsDir/iter"))
    .append(s"elapsed_time=${getElapsedSeconds(t_start)} iter=${fuzzer.Global.iteration}\n")
    .flush()
  }

  def fuzzerInitialize(args: Array[String], f: Array[String] => Unit, f_mutant: Array[String] => Unit): Unit = {
    outDir = args(0)
    testCaseOutDir = s"$outDir/interesting-inputs"
    coverageOutDir = s"$outDir/scoverage-results"
    refCoverageOutDir = s"$coverageOutDir/referenceProgram"
    createDirs(testCaseOutDir)
    createDirs(refCoverageOutDir)
    t_start = System.currentTimeMillis()
    refProgram = new Program("ref","","",f,Array())
    mutantProgram = new Program("mutant","","",f_mutant,Array())
  }

  def fuzzTestOneInputMutant(
                        data: FuzzedDataProvider,
                        f: Array[String] => Unit,
                        f_mutant: Array[String] => Unit,
                        measurementsDir: String,
                        datasets: Array[String]): Unit = {

    var outDirTestCase = s"$testCaseOutDir/iter_${fuzzer.Global.iteration}"
    val mutated_files = createMutatedDatasets(data, datasets.map(n => s"$outDirTestCase/$n"))
    updateIteration(refCoverageOutDir)

    if (!mutantKilled) {
      val (same, _refExecStats, _mutantExecStats) = compareExecutions(refProgram, mutantProgram, mutated_files)
      refExecStats = _refExecStats
      mutantExecStats = _mutantExecStats
      mutantKilled = !same
      if (mutantKilled) {
        // handle diverging output
        // probably should stop fuzzing here
        val newOutDirTestCase = s"${outDirTestCase}_diverging"
        new File(outDirTestCase).renameTo(new File(newOutDirTestCase))
        outDirTestCase = newOutDirTestCase
        //          return (stats, t_start, System.currentTimeMillis())
      }
    } else {
      val execInfo = exec(refProgram, mutated_files)
      postMutantKill = true
    }

    val (newStats, newLastCoverage, changed) = analyzeAndLogCoverage(refCoverageOutDir, stats, lastCoverage, t_start)
    //      val (newMutantStats, newMutantLastCoverage, _) = analyzeAndLogCoverage(mutantCoverageOutDir, mutantStats, mutantLastCoverage)

    logTimeAndIteration(outDir, t_start)

    if (changed) {
      logTimeAndIteration(outDirTestCase, t_start)
      writeStringToFile(s"$outDirTestCase/ref_output.stdout", refExecStats.stdout)
      writeStringToFile(s"$outDirTestCase/ref_output.stderr", refExecStats.stderr)
      if (!postMutantKill) {
        writeStringToFile(s"$outDirTestCase/mutant_output.stdout", mutantExecStats.stdout)
        writeStringToFile(s"$outDirTestCase/mutant_output.stderr", mutantExecStats.stderr)
      }
      new File(outDirTestCase).renameTo(new File(s"${outDirTestCase}_newCov_${newLastCoverage}"))
    }

    if (!changed && (!mutantKilled || postMutantKill)) {
      new Directory(new File(outDirTestCase)).deleteRecursively()
    }

    stats = newStats
    lastCoverage = newLastCoverage
    fuzzer.Global.iteration += 1
    SharedJazzerLogic.trackCumulativeCoverage(refCoverageOutDir)
//    var throwable: Throwable = null
//    try {
//      f(newDatasets)
//    }
//    catch { case e: Throwable => throwable = e}
//    finally {  }
//    if (throwable != null)
//      throw throwable
  }

  def fuzzTestOneInput(
                        data: FuzzedDataProvider,
                        f: Array[String] => Unit,
                        measurementsDir: String,
                        datasets: Array[String]): Unit = {

    val newDatasets = createMutatedDatasets(data, datasets)

    updateIteration(measurementsDir)
    
    var throwable: Throwable = null
    try { f(newDatasets) } 
    catch {
      case e: Throwable =>
        throwable = e
    }
    finally {
      SharedJazzerLogic.trackCumulativeCoverage(measurementsDir)
    }

    if (throwable != null)
      throw throwable
  }

  def fuzzTestOneInput(
                        data: FuzzedDataProvider,
                        datasets: Array[String],
                        f: Array[String] => Unit
                      ): Unit = {

    val newDatasets = createMutatedDatasets(data, datasets.map(s => s".$s"))
    f(newDatasets)
  }

  def getElapsedSeconds(t_start: Long): Float = {
    (System.currentTimeMillis() - t_start) / 1000.0f + 1.0f
  }

  def trackCumulativeCoverage(measurementsDir: String): Unit = {
    val coverage = Serializer.deserialize(new File(s"$measurementsDir/scoverage.coverage")) // scoverage.coverage will be produced at compiler time by ScoverageInstrumenter.scala
    val measurementFiles = IOUtils.findMeasurementFiles(measurementsDir)
    val measurements = scoverage.IOUtils.invoked(measurementFiles)
    coverage.apply(measurements)
    if(coverage.statementCoveragePercent > prevCov) {
      new FileWriter(new File(s"$measurementsDir/coverage.tuples"), true)
          .append(s"(${getElapsedSeconds(t_start)},${coverage.statementCoveragePercent}) %iter=${fuzzer.Global.iteration}")
          .append("\n")
          .flush()
      prevCov = coverage.statementCoveragePercent
    }
  }

  def createMeasurementDir(path: String): Unit = {
    new File(path).mkdirs()
  }

  def createDirs(path: String): Unit = {
    new File(path).mkdirs()
  }


  def createMutatedDatasets(provider: FuzzedDataProvider, datasets: Array[String]): Array[String] = {
    val toConsume = provider.remainingBytes()/datasets.length
    datasets.map{ path => createMutatedDataset(provider, path, toConsume) }
  }

  def createMutatedDataset(provider: FuzzedDataProvider, path: String, toConsume: Int): String = {
    val data = provider.consumeAsciiString(toConsume)
    FileUtils.writeToFile(Seq(data), s"$path/part-00000")
    path
  }
}
