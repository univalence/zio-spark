package zio.spark.internal.codegen.generation.template

import zio.spark.internal.codegen.generation.Environment
import zio.spark.internal.codegen.generation.template.Helper.*

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
