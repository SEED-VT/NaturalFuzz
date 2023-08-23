package utils

import symbolicexecution.SymbolicTree

@SerialVersionUID(2L)
class Query(val queryFunc: Array[Seq[String]] => Array[Seq[String]], val locs: RDDLocations, val tree: SymbolicTree) extends Serializable {
  def offsetLocs(ds: Int, length: Int): Query = {
    new Query(queryFunc, locs.offsetLocs(ds, length), tree.offsetLocs(ds, length))
  }

  def runQuery(rdds: Array[Seq[String]]): QueryResult = {
    new QueryResult(queryFunc(rdds), Seq(), locs)
  }

  override def hashCode(): Int = {
    tree.hashCode()
  }

  override def equals(obj: Any): Boolean = {
    obj.hashCode() == this.hashCode()
  }

  def involvesDS(ds: Int): Boolean = {
    locs.involvesDS(ds)
  }

  def isMultiDatasetQuery: Boolean = {
    locs.isMultiDataset
  }

  def isDummyQuery: Boolean = tree == null

}

object Query {
  def dummyQuery(): Query = {
    new Query(rdds => rdds, new RDDLocations(Array()), SymbolicTree.nopTree())
  }
}
