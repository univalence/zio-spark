package zio.spark.test.internal

import org.apache.spark.sql.types.StructType

import zio.spark.test.ExpectError.WrongSchemaDefinition

final case class SchemaMatcher(columns: Seq[ColumnDescription]) {
  def definitionToSchemaIndex(schema: StructType): Either[WrongSchemaDefinition, Map[Int, Int]] = {
    val acc: Either[WrongSchemaDefinition, Map[Int, Int]] = Right(Map.empty)

    columns.zipWithIndex.foldLeft(acc) { case (acc, (description, definitionIndex)) =>
      val matchingFieldWithIndex = schema.zipWithIndex.find(_._1.name == description.name)

      acc match {
        case Left(error) =>
          matchingFieldWithIndex match {
            case Some(_) => Left(error)
            case None    => Left(error.add(description))
          }
        case Right(currentMap) =>
          matchingFieldWithIndex match {
            case Some((_, schemaIndex)) => Right(currentMap + (definitionIndex -> schemaIndex))
            case None                   => Left(WrongSchemaDefinition(description))
          }
      }
    }
  }
}
