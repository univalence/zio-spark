package zio.spark.internal.codegen.generation.template

import zio.spark.internal.codegen.generation.{Environment, MethodType}
import zio.spark.internal.codegen.generation.template.Helper.*
import zio.spark.internal.codegen.structure.Method

case object DataFrameStatFunctionsTemplate extends Template.Default {
  override def name: String = "DataFrameStatFunctions"

  override def imports(environment: Environment): Option[String] =
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
