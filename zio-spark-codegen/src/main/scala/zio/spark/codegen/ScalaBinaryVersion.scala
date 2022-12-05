package zio.spark.codegen

import sbt.File

sealed trait ScalaBinaryVersion {
  self =>
  override def toString: String =
    self match {
      case ScalaBinaryVersion.V2_11 => "2.11"
      case ScalaBinaryVersion.V2_12 => "2.12"
      case ScalaBinaryVersion.V2_13 => "2.13"
      case ScalaBinaryVersion.V3 => "3"
    }
}

object ScalaBinaryVersion {
  case object V2_11 extends ScalaBinaryVersion

  case object V2_12 extends ScalaBinaryVersion

  case object V2_13 extends ScalaBinaryVersion

  case object V3 extends ScalaBinaryVersion

  def versioned(file: File, scalaVersion: ScalaBinaryVersion): File = new File(file.getPath + "-" + scalaVersion)
}
