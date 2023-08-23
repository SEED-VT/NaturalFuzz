package capture

object IOStreams {

  def _println(x: Any): Unit = {
    val str = flatAny(x)
    fuzzer.Global.stdout += s"${str}\n"
    println(str)
  }

  def flatProduct(t: Product): Iterator[Any] = {
    t.productIterator.flatMap {
      case p: Product => flatProduct(p)
      case l: List[Any] => Iterator(l.mkString(","))
      case a @ Array(_*) => Iterator(a.mkString(","))
      case x => Iterator(x)
    }
  }

  def flatAny(x: Any): String = {
    x match {
      case l: List[Any] => l.mkString(",")
      case a @ Array(_*) => a.mkString(",")
      case p: Product => flatProduct(p).mkString(",")
      case other => Iterator(other).mkString(",")
    }
  }
}

