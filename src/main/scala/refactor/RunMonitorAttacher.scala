package refactor

import refactor.CodeTransformUtils.{treeFromFile,writeTransformed}

import java.io.File
import scala.meta._

object RunMonitorAttacher {
  def main(args: Array[String]): Unit = {
    val outputFolder = "src/main/scala/examples/symbolic"
    val inputFolder = "src/main/scala/examples/tpcds"
    val testName = "Q20"
    new File(outputFolder).mkdirs()
    val inputFile = s"$inputFolder/$testName.scala"

    println("-"*3 + s" $testName " + "-"*10)
    val outputFile = s"$outputFolder/$testName.scala"
    val tree = treeFromFile(inputFile)
    val transformed = MonitorAttacher.attachMonitors(tree)
    println(tree.structure)
    println(transformed.structure)
    writeTransformed(transformed.toString(), outputFile)

  }
}
