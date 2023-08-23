package symbolicexecution

import provenance.data.{DummyProvenance, Provenance}

class SymbolicFloat(override val expr: SymbolicTree, val concrete: Option[Float], val prov: Provenance) extends SymbolicExpression(expr) {

  def this(i: Float) = {
    this(SymbolicTree(null, new ConcreteValueNode(i), null), Some(i), DummyProvenance.create())
  }

  def this(i: Float, provenance: Provenance) = {
    this(SymbolicTree(null, new ProvValueNode(i, provenance), null), Some(i), provenance)
  }

}
