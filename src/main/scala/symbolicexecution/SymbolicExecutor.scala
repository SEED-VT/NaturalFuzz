package symbolicexecution

import fuzzer.SymbolicProgram
import org.apache.spark.util.CollectionAccumulator

import scala.collection.mutable.ListBuffer

object SymbolicExecutor {

  def execute(program: SymbolicProgram, expressionAccumulator: CollectionAccumulator[SymbolicExpression]): SymExResult = {
    program.main(program.args, expressionAccumulator)
  }
}
