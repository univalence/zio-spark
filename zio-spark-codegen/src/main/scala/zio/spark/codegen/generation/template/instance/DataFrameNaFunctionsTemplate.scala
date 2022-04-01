package zio.spark.codegen.generation.template.instance

import zio.spark.codegen.ScalaBinaryVersion
import zio.spark.codegen.generation.MethodType
import zio.spark.codegen.generation.MethodType.GetWithAnalysis
import zio.spark.codegen.generation.template.{Helper, Template}
import zio.spark.codegen.generation.template.Helper.*
import zio.spark.codegen.structure.Method

case object DataFrameNaFunctionsTemplate extends Template.Default {
  override def name: String = "DataFrameNaFunctions"

  override def imports(scalaVersion: ScalaBinaryVersion): Option[String] =
    Some {
      """import org.apache.spark.sql.{
        | Dataset => UnderlyingDataset, 
        | DataFrameNaFunctions => UnderlyingDataFrameNaFunctions
        |}""".stripMargin
    }

  override def helpers: Helper = unpacks

  override def getMethodType(method: Method): MethodType = {
    val baseMethodType = super.getMethodType(method)

    method.name match {
      case "as" => GetWithAnalysis
      case _    => baseMethodType
    }
  }
}
