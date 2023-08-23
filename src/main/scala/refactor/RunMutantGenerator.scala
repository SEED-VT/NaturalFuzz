package refactor

import refactor.CodeTransformUtils.{treeFromFile,writeTransformed}
import java.io.File
import scala.meta._
import utils.FileUtils.writeToFile

object RunMutantGenerator {
  def main(args: Array[String]): Unit = {
    val inputFolder = "src/main/scala/examples/faulty"
    val testNames = List("Q6", "Q7", "Q12", "Q15", "Q19", "Q20")
    testNames.foreach {
      testName =>
        val outputFolder = s"src/main/scala/examples/mutants/${testName}"
        new File(outputFolder).mkdirs()
        val inputFile = s"$inputFolder/$testName.scala"
        val tree = treeFromFile(inputFile)


        val mutants = MutantGenerator.generateMutants(tree, testName)
        mutants
          .zipWithIndex
          .foreach {
          case ((transformed, mutantSuffix, mutantInfo), i) =>
            writeToFile(Seq(s"$mutantInfo"), s"$outputFolder/$mutantSuffix.info")
            writeTransformed(transformed.toString(), s"$outputFolder/${testName}_$mutantSuffix.scala")
        }
    }

  }
}
