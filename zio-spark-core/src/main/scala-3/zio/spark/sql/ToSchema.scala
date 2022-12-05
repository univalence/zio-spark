package zio.spark.sql

import magnolia1.*
import org.apache.spark.sql.types.*

object SchemaFromCaseClass {
  type ToStructSchema[T] = zio.spark.sql.ToStructSchema[T]
}

trait ToSchema[T] {
  def toSchema: DataType
  def isNullable: Boolean = false
}

final case class NullableToSchema[T](ts: ToSchema[T]) extends ToSchema[Option[T]] {
  def toSchema: DataType           = ts.toSchema
  override def isNullable: Boolean = true
}

trait ToStructSchema[T] extends ToSchema[T] {
  override def toSchema: StructType
}

object ToSchema extends AutoDerivation[ToSchema] {
  def apply[T](dataType: DataType): ToSchema[T] =
    new ToSchema[T] {
      def toSchema: DataType = dataType
    }

  override def join[T](cc: CaseClass[ToSchema, T]): ToStructSchema[T] =
    new ToStructSchema[T] {
      def toSchema: StructType = {
        val fields: List[StructField] =
          cc.params
            .map(p => StructField(p.label, p.typeclass.toSchema, nullable = p.typeclass.isNullable))
            .toList

        StructType(fields)
      }
    }


  given [T <: Any & Product](using T: ToSchema[T]):ToStructSchema[T] = T.asInstanceOf[ToStructSchema[T]]

  // We should not split a ToStructSchema.
  override def split[T](ctx: SealedTrait[ToSchema, T]): ToSchema[T] = ???

  given ToSchema[String] = ToSchema(StringType)

  given ToSchema[Boolean] = ToSchema(BooleanType)

  given ToSchema[Short] = ToSchema(ShortType)
  given ToSchema[Int] = ToSchema(IntegerType)
  given ToSchema[Long] = ToSchema(LongType)

  given ToSchema[java.math.BigDecimal] = ToSchema(DecimalType(10, 0))

  given ToSchema[Float]   = ToSchema(FloatType)
  given ToSchema[Double] = ToSchema(DoubleType)

  given ToSchema[Byte] = ToSchema(ByteType)
  given ToSchema[Array[Byte]] = ToSchema(BinaryType)

  given ToSchema[java.sql.Timestamp] = ToSchema(TimestampType)
  given ToSchema[java.sql.Date] = ToSchema(DateType)

  given [T](using T: ToSchema[T]): ToSchema[Option[T]] = NullableToSchema(T)
  given [K, V](using K: ToSchema[K], V: ToSchema[V]): ToSchema[Map[K, V]] = ToSchema(DataTypes.createMapType(K.toSchema, V.toSchema))
  given [T](using T: ToSchema[T]): ToSchema[List[T]] = ToSchema(DataTypes.createArrayType(T.toSchema))

  def schemaFrom[T](using T: ToStructSchema[T]): ToStructSchema[T] = T
}

