package utils.reflection

import java.lang.reflect.{Method, Modifier}

object DynamicClassLoader {

  def invokeMethod(className: String, functionName: String, args: Any*): Option[Any] = {
    try {
      val clazz = Class.forName(className)
      val method = findMethod(clazz, functionName, args.map(_.getClass): _*)
      method.setAccessible(true)
      val instance = if (Modifier.isStatic(method.getModifiers)) null else clazz.newInstance()
      Some(method.invoke(instance, args.asInstanceOf[Seq[AnyRef]]: _*))
    } catch {
      case ex: Exception =>
        System.err.println(s"An error occurred while invoking the method: $ex")
        System.err.println(ex.getStackTrace.mkString("\n"))
        System.err.println("---- CAUSED BY ----")
        ex.getCause.printStackTrace()
        None
    }
  }

  def findMethod(clazz: Class[_], methodName: String, argTypes: Class[_]*): Method = {
    clazz
      .getDeclaredMethods.filter(_.getName == methodName)
      .head
  }

}
