package utils

import symbolicexecution.SymExResult

object RIGUtils extends Serializable {
  def createFilterQueries(pathExpressions: SymExResult): FilterQueries = {
    new FilterQueries(pathExpressions)
  }
}
