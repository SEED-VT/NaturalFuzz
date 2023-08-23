package symbolicexecution

import provenance.data.{DummyProvenance, Provenance}

class SymbolicString(override val expr: SymbolicTree, val concrete: Option[String], val prov: Provenance) extends SymbolicExpression(expr) {

  def this(i: String) = {
    this(SymbolicTree(null, new ConcreteValueNode(i), null), Some(i), DummyProvenance.create())
  }

  def this(i: String, provenance: Provenance) = {
    this(SymbolicTree(null, new ProvValueNode(i, provenance), null), Some(i), provenance)
  }

}
