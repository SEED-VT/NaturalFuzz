package utils
import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}
import java.io.{File, PrintWriter}
import org.json4s._
import org.json4s.jackson.Serialization

object Pickle {
  val scalaVersion = scala.util.Properties.versionString
  val jvmVersion = System.getProperty("java.version")
  def dump(obj: Any, file: String): Unit = {
    println(s"Pickle.dump with scala version: $scalaVersion")
    println(s"Pickle.dump with jvm version: $jvmVersion")
    val outputStream = new ObjectOutputStream(new FileOutputStream(file))
    outputStream.writeObject(obj)
    outputStream.close()
  }

  def load[T](file: String): T = {
    println(s"Pickle.load with scala version: $scalaVersion")
    println(s"Pickle.load with jvm version: $jvmVersion")
    val inputStream = new ObjectInputStream(new FileInputStream(file))
    val obj = inputStream.readObject.asInstanceOf[T]
    inputStream.close()
    obj
  }

  implicit val formats = DefaultFormats

  def serialize(obj: AnyRef, fileName: String): Unit = {
    val file = new File(fileName)
    val pw = new PrintWriter(file)
    pw.write(Serialization.write(obj))
    pw.close()
  }

  def deserialize[T](fileName: String)(implicit mf: Manifest[T]): T = {
    val file = new File(fileName)
    val jsonStr = scala.io.Source.fromFile(file).mkString
    Serialization.read[T](jsonStr)
  }

}
