package utils

object MiscUtils {

  def toBinaryStringWithLeadingZeros(pathVector: Int) = {
    "0" * 32 + pathVector.toBinaryString takeRight 32
  }

}
