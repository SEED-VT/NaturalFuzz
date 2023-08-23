package utils

import fuzzer.Schema

object SchemaUtils {

  def inferType(v: String): Schema[Any] = {
    try {
      v.toDouble
      new Schema[Any](Schema.TYPE_NUMERICAL)
    } catch {
      case _: NumberFormatException => new Schema[Any](Schema.TYPE_OTHER)
    }

  }

}
