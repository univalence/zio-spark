package zio.spark.test.internal

import SchemaMatcher._
import org.apache.spark.sql.types.{DataType, StructType}

final case class SchemaMatcher(columns: Seq[ColumnDescription]) {
  def definitionToSchemaIndex(schema: StructType): Option[Map[Int, Int]] =
    columns.zipWithIndex.foldLeft(Option(Map.empty[Int, Int])) {
      case (acc, (ColumnDescription(name, _), definitionIndex)) =>
        acc match {
          case None => None
          case Some(currentMap) =>
            val matchingFieldWithIndex = schema.zipWithIndex.find(_._1.name == name)

            matchingFieldWithIndex match {
              case Some((_, schemaIndex)) => Some(currentMap + (definitionIndex -> schemaIndex))
              case None                   => None // TODO: Better error management
            }
        }
    }
}

object SchemaMatcher {
  final case class ColumnDescription(name: String, dataType: Option[DataType]) {
    def as(dataType: DataType): ColumnDescription = copy(dataType = Some(dataType))
  }
}
