package zio.spark.codegen.generation

object Error {
  trait CodegenError { self =>
    def msg: String

    def fullMsg: String = s"""${this.getClass.getSimpleName}: $msg""".stripMargin
  }

  abstract class ExceptionError(cause: Throwable) extends CodegenError {
    override def fullMsg: String =
      s"""${super.fullMsg}
         |Cause was (${cause.toString}):
         |${cause.getStackTrace.map("    " + _.toString).mkString("\n")}""".stripMargin
  }

  case class ContentIsNotSourceError(path: String, cause: Throwable) extends ExceptionError(cause) {
    override def msg: String = s"The content of '$path' is not parsable as Scala Source."
  }

  case class FileReadingError(path: String, cause: Throwable) extends ExceptionError(cause) {
    override def msg: String = s"Unable to read '$path'."
  }

  case class SourceNotFoundError(file: String, module: String, cause: Throwable) extends ExceptionError(cause) {
    override def msg: String = s"The sources of the file '$file' of the module '$module' are not found."
  }

  case class WriteError(path: String, cause: Throwable) extends ExceptionError(cause) {
    override def msg: String = s"Can't write the generated code at '$path'."
  }

  case class ModuleNotFoundError(module: String) extends CodegenError {
    override def msg: String = s"The sources of the module '$module' are not found."
  }

  case class UnimplementedMethodsError(methodNames: Set[String]) extends CodegenError {
    override def msg: String =
      s"""The following methods should be implemented in overlays:
         |${methodNames.map(name => s"- $name").mkString("\n")}""".stripMargin
  }

  case class NotIndentedError(lines: Seq[String]) extends CodegenError {
    override def msg: String =
      s"""The following lines are not indented:
         |${lines.map(line => s"- $line").mkString("\n")}""".stripMargin
  }
}
