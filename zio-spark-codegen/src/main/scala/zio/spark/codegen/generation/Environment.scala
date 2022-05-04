package zio.spark.codegen.generation

import org.scalafmt.interfaces.Scalafmt
import sbt.File
import sbt.Keys.Classpath

import zio.{URIO, ZIO}
import zio.spark.codegen.ScalaBinaryVersion
import zio.spark.codegen.ScalaBinaryVersion.versioned

import java.nio.file.Path

/**
 * The environment required by the codegen to work. Generally speaking,
 * it is all sbt settings that we need in the codebase.
 */
object Environment {
  type Environment = Logger & Classpath & ScalaBinaryVersion & ZIOSparkFolders & ScalafmtFormatter

  trait ZIOSparkFolders {
    def mainFolder: File
    def mainFolderVersioned: File
    def itFolder: File
    def itFolderVersioned: File
  }

  object ZIOSparkFolders {
    def mainFolder: URIO[ZIOSparkFolders, File] = ZIO.service[ZIOSparkFolders].map(_.mainFolder)
    def mainFolderVersioned: URIO[ZIOSparkFolders, File] = ZIO.service[ZIOSparkFolders].map(_.mainFolderVersioned)
    def itFolder: URIO[ZIOSparkFolders, File] = ZIO.service[ZIOSparkFolders].map(_.itFolder)
    def itFolderVersioned: URIO[ZIOSparkFolders, File] = ZIO.service[ZIOSparkFolders].map(_.itFolderVersioned)
  }

  case class ZIOSparkFoldersLive(sbtMainFolder: File, scalaVersion: ScalaBinaryVersion) extends ZIOSparkFolders {
    override def mainFolder: File = sbtMainFolder
    def mainFolderVersioned: File = versioned(mainFolder, scalaVersion)
    def itFolder: File            = new File(mainFolder.getPath.replace("main", "it"))
    def itFolderVersioned: File   = versioned(itFolder, scalaVersion)
  }

  trait ScalafmtFormatter {
    def format(code: String, path: Path): String
  }

  object ScalafmtFormatter {
    def format(code: String, path: Path): URIO[ScalafmtFormatter, String] =
      ZIO.service[ScalafmtFormatter].map(_.format(code, path))
  }

  case class ScalafmtFormatterLive(scalafmt: Scalafmt, configuration: Path) extends ScalafmtFormatter {
    override def format(code: String, path: Path): String = scalafmt.format(configuration, path, code)
  }
}
