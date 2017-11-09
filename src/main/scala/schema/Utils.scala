package schema

import java.net.URL

import scala.io.Source

object Utils {

  val att_line = """^\s*(\S*)\s.*$""".r

  def loadSchema(resource: URL): Array[String] = {
    val reader = Source.fromURL(resource).getLines()
    reader
      .drop(1)
      .filter(l => l.trim != ");")
      .map(l => att_line.replaceAllIn(l, "$1"))
      .toArray
  }

}
