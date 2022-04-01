package zio.spark.codegen.generation

import org.scalafmt.interfaces.Scalafmt
import sbt.File
import sbt.Keys.Classpath

import zio.spark.codegen.ScalaBinaryVersion
import zio.spark.codegen.ScalaBinaryVersion.versioned

import java.nio.file.Path

/**
 * The environment required by the codegen to work. Generally speaking,
 * it is all sbt settings that we need in the codebase.
 * @param mainFolder
 *   The path of the "main" folder containing the zio spark sources
 * @param classpath
 *   The java classpath containing the spark sources
 * @param scalaVersion
 *   The current scala version
 * @param scalafmt
 *   The scalafmt instance
 * @param scalafmtConfiguration
 *   The path of the scalafmt configuration
 */
case class Environment(
    mainFolder:            File,
    classpath:             Classpath,
    scalaVersion:          ScalaBinaryVersion,
    scalafmt:              Scalafmt,
    scalafmtConfiguration: Path
) {
  def mainFolderVersioned: File = versioned(mainFolder, scalaVersion)
  def itFolder: File            = new File(mainFolder.getPath.replace("main", "it"))
  def itFolderVersioned: File   = versioned(itFolder, scalaVersion)
}
