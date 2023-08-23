package symbolicexecution

import provenance.data.{DummyProvenance, Provenance}

class SymbolicInteger(override val expr: SymbolicTree, val concrete: Option[Int], val prov: Provenance) extends SymbolicExpression(expr) {

  def this(i: Int) = {
    this(SymbolicTree(null, new ConcreteValueNode(i), null), Some(i), DummyProvenance.create())
  }

  def this(i: Int, provenance: Provenance) = {
    this(SymbolicTree(null, new ProvValueNode(i, provenance), null), Some(i), provenance)
  }

}
