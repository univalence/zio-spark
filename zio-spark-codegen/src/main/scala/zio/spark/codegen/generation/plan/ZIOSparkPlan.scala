package zio.spark.codegen.generation.plan
import sbt.fileToRichFile

import zio.{URIO, ZIO}
import zio.spark.codegen.generation.Environment.{Environment, ZIOSparkFolders}
import zio.spark.codegen.generation.Error.CodegenError
import zio.spark.codegen.generation.Loader.{optionalSourceFromFile, sourceFromFile}
import zio.spark.codegen.structure.Helpers.mergeSources

import java.nio.file.Path

case class ZIOSparkPlan(name: String, hierarchy: String) extends Plan {
  val path: String = hierarchy.replace(".", "/")

  override def generatePath: URIO[Environment, Path] =
    for {
      folders <- ZIO.service[ZIOSparkFolders]
      file = folders.mainFolderVersioned / path / s"$name.scala"
    } yield file.toPath

  override def generateCode: ZIO[Environment, CodegenError, String] =
    for {
      zioSparkFolders <- ZIO.service[ZIOSparkFolders]
      basePath     = zioSparkFolders.itFolder / s"$name.scala"
      specificPath = zioSparkFolders.itFolderVersioned / s"${name}Specific.scala"
      baseSource          <- sourceFromFile(basePath)
      maybeSpecificSource <- optionalSourceFromFile(specificPath)
      mergedSources =
        maybeSpecificSource match {
          case None                 => baseSource.syntax
          case Some(specificSource) => mergeSources(baseSource, specificSource)
        }
      specificImports =
        maybeSpecificSource
          .map(_.syntax.split("\n").toList)
          .getOrElse(List.empty)
          .filter(_.startsWith("import"))
          .filter(!_.startsWith(s"import $hierarchy._")) // This import is not required anymore
          .mkString("\n")
      finalSources =
        mergedSources
          .split("\n")
          .filter(!_.startsWith(s"import $hierarchy._")) // This import is not required anymore
          .mkString("\n")
    } yield s"""package $hierarchy
               |
               |$specificImports
               |$finalSources""".stripMargin
}
