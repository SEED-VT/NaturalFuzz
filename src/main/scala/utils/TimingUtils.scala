package utils

object TimingUtils {

  def timeFunction[T](f: () => T): (T, Long) = {
    val tStart = System.currentTimeMillis()
    (f(), System.currentTimeMillis() - tStart)
  }

}
