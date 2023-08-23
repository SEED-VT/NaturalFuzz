package symbolicexecution

class SymbolicBoolean(val concrete: Boolean) {

  def toBoolean: Boolean = { concrete }

}
