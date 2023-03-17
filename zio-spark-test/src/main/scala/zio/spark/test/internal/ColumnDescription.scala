package zio.spark.test.internal

import org.apache.spark.sql.types.DataType

final case class ColumnDescription(name: String, dataType: Option[DataType]) {
  def as(dataType: DataType): ColumnDescription = copy(dataType = Some(dataType))

  override def toString: String =
    dataType match {
      case Some(value) => s"$name of type ${value.typeName}"
      case None        => s"$name"
    }
}
