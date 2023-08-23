
package examples.benchmarks

import java.io._
import scala.io.Source

object LineCounter {

  def main(args: Array[String]): Unit = {
    val listOfFilenamesFile = "filenames.txt"

    // turn the “list of filenames” file into a list
    val listOfFilenames = Source.fromFile(listOfFilenamesFile).getLines.toList

    println(listOfFilenames)
    var linesOfSourceCode = 0
    var inSourceCodeSection = false

    // each filename is an Adoc file
    for (filename <- listOfFilenames) {
      println(filename)
      val adocLines = Source.fromFile(filename).getLines.toList
      for (line <- adocLines) {
        if (line.matches("^----$") || line.matches("^\\.\\.\\.\\.$")) {
          // if (line.matches("^\\.\\.\\.\\.$")) {
          // matched a line like "-----" or "....", so toggle this
          if (inSourceCodeSection == false)
            inSourceCodeSection = true
          else
            inSourceCodeSection = false
        } else {
          if (inSourceCodeSection && line.trim != "" && line.trim.startsWith("//") == false) {
            linesOfSourceCode += 1 // <-- what you really want
            println(line) // <-- debug
          }
        }
      }

    }
    println(s"TOTAL COUNT: $linesOfSourceCode")
  }

}