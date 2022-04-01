package zio.spark.codegen.generation

import org.scalafmt.interfaces.Scalafmt
import sbt.File
import sbt.Keys.Classpath

import zio.{Accessible, Console}
import zio.spark.codegen.ScalaBinaryVersion
import zio.spark.codegen.ScalaBinaryVersion.versioned

import java.nio.file.Path

/**
 * The environment required by the codegen to work. Generally speaking,
 * it is all sbt settings that we need in the codebase.
 */
object Environment {
  type Environment = Console & Classpath & ScalaBinaryVersion & ZIOSparkFolders & ScalafmtFormatter

  trait ZIOSparkFolders {
    def mainFolder: File
    def mainFolderVersioned: File
    def itFolder: File
    def itFolderVersioned: File
  }

  object ZIOSparkFolders extends Accessible[ZIOSparkFolders]

  case class ZIOSparkFoldersLive(sbtMainFolder: File, scalaVersion: ScalaBinaryVersion) extends ZIOSparkFolders {
    override def mainFolder: File = sbtMainFolder
    def mainFolderVersioned: File = versioned(mainFolder, scalaVersion)
    def itFolder: File            = new File(mainFolder.getPath.replace("main", "it"))
    def itFolderVersioned: File   = versioned(itFolder, scalaVersion)
  }

  trait ScalafmtFormatter {
    def format(code: String, path: Path): String
  }

  object ScalafmtFormatter extends Accessible[ScalafmtFormatter]

  case class ScalafmtFormatterLive(scalafmt: Scalafmt, configuration: Path) extends ScalafmtFormatter {
    override def format(code: String, path: Path): String = scalafmt.format(configuration, path, code)
  }
}
