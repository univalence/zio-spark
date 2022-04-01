package zio.spark.codegen.generation.template

import zio.spark.codegen.generation.Environment
import zio.spark.codegen.generation.template.Helper.*

case object DataFrameNaFunctionsTemplate extends Template.Default {
  override def name: String = "DataFrameNaFunctions"

  override def imports(environment: Environment): Option[String] =
    Some {
      """import org.apache.spark.sql.{
        | Dataset => UnderlyingDataset, 
        | DataFrameNaFunctions => UnderlyingDataFrameNaFunctions
        |}""".stripMargin
    }

  override def helpers: Helper = unpacks
}
