package zio.spark.sql

import magnolia1.*
import org.apache.spark.sql.types.*

object SchemaFromCaseClass {

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

  object ToSchema {
    def apply[T](dataType: DataType): ToSchema[T] =
      new ToSchema[T] {
        def toSchema: DataType = dataType
      }
  }

  object ToStructSchema extends AutoDerivation[ToSchema] {
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

    // We should not split a ToStructSchema.
    override def split[T](ctx: SealedTrait[ToSchema, T]): ToSchema[T] = ???



    implicit def schemaFrom[A]: ToStructSchema[A] = macro Magnolia.gen[A]

  }

  implicit val stringToSchema: ToSchema[String] = ToSchema(StringType)

  implicit val booleanToSchema: ToSchema[Boolean] = ToSchema(BooleanType)

  implicit val shortToSchema: ToSchema[Short] = ToSchema(ShortType)
  implicit val integerToSchema: ToSchema[Int] = ToSchema(IntegerType)
  implicit val longToSchema: ToSchema[Long]   = ToSchema(LongType)

  implicit val bigDecimalToSchema: ToSchema[BigDecimal]               = ToSchema(DecimalType(10, 0))
  implicit val bigDecimalJavaToSchema: ToSchema[java.math.BigDecimal] = ToSchema(DecimalType(10, 0))

  implicit val floatToSchema: ToSchema[Float]   = ToSchema(FloatType)
  implicit val doubleToSchema: ToSchema[Double] = ToSchema(DoubleType)

  implicit val byteToSchema: ToSchema[Byte]          = ToSchema(ByteType)
  implicit val binaryToSchema: ToSchema[Array[Byte]] = ToSchema(BinaryType)

  implicit val timestampToSchema: ToSchema[java.sql.Timestamp] = ToSchema(TimestampType)
  implicit val dateToSchema: ToSchema[java.sql.Date]           = ToSchema(DateType)

  implicit def optToSchema[T](implicit T: ToSchema[T]): ToSchema[Option[T]] = NullableToSchema(T)

  implicit def mapToSchema[K, V](implicit K: ToSchema[K], V: ToSchema[V]): ToSchema[Map[K, V]] =
    ToSchema(DataTypes.createMapType(K.toSchema, V.toSchema))

  implicit def listToSchema[T](implicit T: ToSchema[T]): ToSchema[List[T]] =
    ToSchema(DataTypes.createArrayType(T.toSchema))

  type Typeclass[A] = ToSchema[A]


  implicit class schemaOps[A <: AnyRef with Product](a: A) {
    def schema(implicit toStructSchema: ToStructSchema[A]): StructType = toStructSchema.toSchema
  }
}
