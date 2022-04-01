package zio.spark.codegen.structure

case class SurroundedString(prefix: Option[String], content: String, suffix: Option[String]) {
  def >>>(suffix: String): SurroundedString = copy(suffix = Some(suffix))

  override def toString: String =
    if (content.nonEmpty) prefix.map(_ + "\n").getOrElse("") + content + suffix.map("\n" + _).getOrElse("")
    else ""
}

object SurroundedString {
  implicit class stringWrapper(string: String) {
    def <<<(content: String): SurroundedString = SurroundedString(Some(string), content, None)

    def >>>(prefix: String): SurroundedString = SurroundedString(None, string, Some(prefix))
  }
}
