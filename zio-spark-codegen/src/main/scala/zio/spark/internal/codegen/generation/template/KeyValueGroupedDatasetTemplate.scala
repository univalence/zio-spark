package zio.spark.internal.codegen.generation.template

import zio.spark.internal.codegen.generation.{Environment, MethodType}
import zio.spark.internal.codegen.generation.MethodType.*
import zio.spark.internal.codegen.generation.template.Helper.*
import zio.spark.internal.codegen.structure.Method

case object KeyValueGroupedDatasetTemplate extends Template.Default {
  override def name: String = "KeyValueGroupedDataset"

  override def typeParameters: List[String] = List("K", "V")

  override def imports(environment: Environment): Option[String] =
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
