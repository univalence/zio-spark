package zio.spark.codegen.generation.template.instance

import zio.spark.codegen.ScalaBinaryVersion
import zio.spark.codegen.generation.MethodType
import zio.spark.codegen.generation.MethodType.{GetWithAnalysis, Unpack}
import zio.spark.codegen.generation.template.{Helper, Template}
import zio.spark.codegen.generation.template.Helper.*
import zio.spark.codegen.structure.Method

case object KeyValueGroupedDatasetTemplate extends Template.Default {
  override def name: String = "KeyValueGroupedDataset"

  override def typeParameters: List[String] = List("K", "V")

  override def imports(scalaVersion: ScalaBinaryVersion): Option[String] =
    Some {
      """import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout, OutputMode}
        |import org.apache.spark.sql.{
        |  Encoder, 
        |  TypedColumn, 
        |  Dataset => UnderlyingDataset, 
        |  KeyValueGroupedDataset => UnderlyingKeyValueGroupedDataset
        |}
        |""".stripMargin
    }

  override def helpers: Helper = unpacks && transformation

  override def getMethodType(method: Method): MethodType = {
    val baseMethodType = super.getMethodType(method)

    method.name match {
      case "as"    => GetWithAnalysis
      case "count" => Unpack
      case _       => baseMethodType
    }
  }
}
