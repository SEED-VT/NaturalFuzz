package refactor

import utils.MutationUtils.getRandomElement

import scala.meta.Term.ApplyInfix.unapply
import scala.meta.{Defn, Term, Transformer, Tree}
import scala.util.Random

object Cleaner extends Transformer {
  override def apply(tree: Tree): Tree = {
    tree match {
      case node @ _ =>
        super.apply(node)
    }
  }
}



