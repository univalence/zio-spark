package zio.spark.codegen.generation.template.instance

import zio.spark.codegen.ScalaBinaryVersion
import zio.spark.codegen.generation.MethodType
import zio.spark.codegen.generation.MethodType.{Ignored, Transformation}
import zio.spark.codegen.generation.template.{Helper, Template}
import zio.spark.codegen.generation.template.Helper.*
import zio.spark.codegen.structure.Method

case object DatasetTemplate extends Template.Default {
  override def name: String = "Dataset"

  override def typeParameters: List[String] = List("T")

  override def imports(scalaVersion: ScalaBinaryVersion): Option[String] =
    Some {
      val baseImports: String =
        """import org.apache.spark.sql.{
          |  Column,
          |  Dataset => UnderlyingDataset,
          |  DataFrameNaFunctions => UnderlyingDataFrameNaFunctions,
          |  DataFrameStatFunctions => UnderlyingDataFrameStatFunctions,
          |  RelationalGroupedDataset => UnderlyingRelationalGroupedDataset,
          |  KeyValueGroupedDataset => UnderlyingKeyValueGroupedDataset,
          |  Encoder,
          |  Row,
          |  TypedColumn,
          |  Sniffer
          |}
          |import zio.spark.sql.streaming.DataStreamWriter
          |import org.apache.spark.sql.types.StructType
          |import org.apache.spark.storage.StorageLevel
          |
          |import zio._
          |import zio.spark.rdd._
          |
          |import scala.reflect.runtime.universe.TypeTag
          |
          |import java.io.IOException
          |""".stripMargin

      scalaVersion match {
        case ScalaBinaryVersion.V2_13 | ScalaBinaryVersion.V3 =>
          s"""$baseImports
             |import org.apache.spark.sql.execution.ExplainMode
             |import scala.jdk.CollectionConverters._""".stripMargin
        case ScalaBinaryVersion.V2_12 =>
          s"""$baseImports
             |import org.apache.spark.sql.execution.ExplainMode
             |import scala.collection.JavaConverters._""".stripMargin
        case ScalaBinaryVersion.V2_11 =>
          s"""$baseImports
             |import org.apache.spark.sql.execution.command.ExplainCommand
             |import scala.collection.JavaConverters._""".stripMargin
      }
    }

  override def implicits(scalaVersion: ScalaBinaryVersion): Option[String] =
    Some {
      s"""private implicit def lift[U](x:Underlying$name[U]):$name[U] = 
         |  $name(x)
         |private implicit def iteratorConversion[U](iterator: java.util.Iterator[U]):Iterator[U] =
         |  iterator.asScala
         |private implicit def liftDataFrameNaFunctions(x: UnderlyingDataFrameNaFunctions): DataFrameNaFunctions =
         |  DataFrameNaFunctions(x)
         |private implicit def liftDataFrameStatFunctions(x: UnderlyingDataFrameStatFunctions): DataFrameStatFunctions =
         |  DataFrameStatFunctions(x)
         |private implicit def liftRelationalGroupedDataset[U](x: UnderlyingRelationalGroupedDataset): RelationalGroupedDataset =
         |  RelationalGroupedDataset(x)
         |private implicit def liftKeyValueGroupedDataset[K, V](x: UnderlyingKeyValueGroupedDataset[K, V]): KeyValueGroupedDataset[K, V] =
         |  KeyValueGroupedDataset(x)""".stripMargin
    }

  override def helpers: Helper = action && transformations && gets

  override def getMethodType(method: Method): MethodType = {
    val baseMethodType = super.getMethodType(method)

    method.name match {
      case "apply"                           => Ignored
      case "drop"                            => Transformation
      case "col" | "colRegex" | "withColumn" => baseMethodType.withAnalysis
      case _                                 => baseMethodType
    }
  }
}
