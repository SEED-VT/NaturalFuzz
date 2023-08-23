package testfiles

object Test6 {
  def main(args: Array[String]): Unit = {
    val clazz = "examples.mutants.WebpageSegmentation.WebpageSegmentation_M0_18_minus_times"
    val runtimeClass = Class.forName(clazz)
    val method = runtimeClass.getMethod("main", classOf[Array[String]])
    val results = method.invoke(runtimeClass, Array("before", "after").map { s => s"seeds/reduced_data/webpage_segmentation/$s" })

  }
}
