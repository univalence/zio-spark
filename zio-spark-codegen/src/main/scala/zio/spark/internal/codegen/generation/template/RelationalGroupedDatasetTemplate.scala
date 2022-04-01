package zio.spark.internal.codegen.generation.template

import zio.spark.internal.codegen.ScalaBinaryVersion
import zio.spark.internal.codegen.generation.{Environment, MethodType}
import zio.spark.internal.codegen.generation.MethodType.*
import zio.spark.internal.codegen.generation.template.Helper.*
import zio.spark.internal.codegen.structure.Method

case object RelationalGroupedDatasetTemplate extends Template.Default {
  override def name: String = "RelationalGroupedDataset"

  override def imports(environment: Environment): Option[String] =
    Some {
      environment.scalaVersion match {
        case ScalaBinaryVersion.V2_11 =>
          """import org.apache.spark.sql.{
            |  Column,
            |  Dataset => UnderlyingDataset,
            |  RelationalGroupedDataset => UnderlyingRelationalGroupedDataset
            |}
            |""".stripMargin
        case _ =>
          """import org.apache.spark.sql.{
            |  Column,
            |  Encoder,
            |  Dataset => UnderlyingDataset,
            |  RelationalGroupedDataset => UnderlyingRelationalGroupedDataset,
            |  KeyValueGroupedDataset => UnderlyingKeyValueGroupedDataset,
            |}
            |""".stripMargin
      }
    }

  override def implicits(environment: Environment): Option[String] =
    Some {
      environment.scalaVersion match {
        case ScalaBinaryVersion.V2_11 => ""
        case _ =>
          s"""implicit private def liftKeyValueGroupedDataset[K, V](
             |  x: UnderlyingKeyValueGroupedDataset[K, V]
             |): KeyValueGroupedDataset[K, V] = KeyValueGroupedDataset(x)""".stripMargin
      }
    }

  override def helpers: Helper = unpacks && transformations && gets

  override def getMethodType(method: Method): MethodType = {
    val baseMethodType = super.getMethodType(method)

    method.name match {
      case "count"                      => Unpack
      case "min" | "max" | "withColumn" => UnpackWithAnalysis
      case _                            => baseMethodType
    }
  }
}
