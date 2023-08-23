package refactor

import java.nio.file.Files
import scala.meta.{Defn, Import, Importer, Init, Input, Name, Source, Stat, Term, Transformer, Tree, Type, XtensionParseInputLike}

object CodeTransformUtils {

  def treeFromFile(p: String): Tree = {
    val path = java.nio.file.Paths.get(p)
    val bytes = java.nio.file.Files.readAllBytes(path)
    val text = new String(bytes, "UTF-8")
    val input = Input.VirtualFile(path.toString, text)
    input.parse[Source].get
  }

  def writeTransformed(code: String, p: String) = {
    val path = java.nio.file.Paths.get(p)
    Files.write(path, code.getBytes())
  }

  def insertAtEndOfFunction(mainFunc: Defn, statement: Stat): Defn = {
    val Defn.Def(mods, name, tparams, paramss, decltpe, Term.Block(code)) = mainFunc
    Defn.Def(mods, name, tparams, paramss, decltpe, Term.Block(code :+ statement))
  }

  def addImports(imports: List[Importer], toAdd: List[String]): List[Importer] = {
    imports ++ toAdd.map(_.parse[Importer].get)
  }

}
