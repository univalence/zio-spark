package zio.spark.codegen.generation.template.instance

import zio.spark.codegen.ScalaBinaryVersion
import zio.spark.codegen.generation.MethodType
import zio.spark.codegen.generation.template.{Helper, Template}
import zio.spark.codegen.generation.template.Helper.*
import zio.spark.codegen.structure.Method

case object DataFrameStatFunctionsTemplate extends Template.Default {
  override def name: String = "DataFrameStatFunctions"

  override def imports(scalaVersion: ScalaBinaryVersion): Option[String] =
    Some {
      """import org.apache.spark.sql.{
        |  Column,
        |  Dataset => UnderlyingDataset, 
        |  DataFrameStatFunctions => UnderlyingDataFrameStatFunctions
        |}
        |import org.apache.spark.util.sketch.{BloomFilter, CountMinSketch}
        |""".stripMargin
    }

  override def helpers: Helper = unpacks && transformations && gets

  override def getMethodType(method: Method): MethodType = {
    val baseMethodType = super.getMethodType(method)

    method.name match {
      case "bloomFilter" | "corr" | "countMinSketch" | "cov" => baseMethodType.withAnalysis
      case _                                                 => baseMethodType
    }
  }
}
