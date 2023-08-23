package testfiles

object Test4 {

  def main(args: Array[String]): Unit = {
    println(f(3.+))
  }

  def f(g: Int => Int): Int = {
    g(2)
  }


}
