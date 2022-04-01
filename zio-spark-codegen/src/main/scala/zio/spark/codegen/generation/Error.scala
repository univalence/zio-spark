package zio.spark.codegen.generation

object Error {
  sealed trait CodegenError {
    def forHuman: String
  }
  case class ContentIsNotSourceError(path: String) extends CodegenError {
    override def forHuman: String = s"The content of $path is not parsable as Scala Source."
  }
  case class FileReadingError(path: String) extends CodegenError {
    override def forHuman: String = s"Unable to read $path."
  }
  case class ModuleNotFoundError(module: String) extends CodegenError {
    override def forHuman: String = s"The sources of the module $module are not found."
  }
  case class SourceNotFoundError(file: String, module: String) extends CodegenError {
    override def forHuman: String = s"The sources of the file $file of the module $module are not found."
  }
  case class UnimplementedMethodsError(methodNames: Set[String]) extends CodegenError {
    override def forHuman: String =
      s"""The following methods should be implemented in overlays:
         |${methodNames.map(name => s"- $name").mkString("\n")}""".stripMargin
  }
  case object NotIndentedError extends CodegenError {
    override def forHuman: String = "The generated code contains non indented line of code."
  }
}
