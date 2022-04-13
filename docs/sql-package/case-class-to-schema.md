---
sidebar_position: 3
---

# Schemas

A DataFrame in Spark always have a schema. 

Generally, people have two ways to create it:
- They infer the schema from the source files (not recommended since the schema can change and because the inference can 
  be wrong)
- They create a StructType and provide it to the DataFrameReader

Defining your schema is a best practice, and you should always do that. However, creating a StructType is annoying
and this is even more true when you want to deal with a Dataset[T]. 

For example, imagine you have the following CSV:

```csv
name;age;phone
john;10;
fella;30;+3360000000
```

If you want to manipulate it as a Dataset[Person] you will have to write something like:

```scala
import zio.spark.sql._
import org.apache.spark.sql.types._

case class Person(name: String, age: Int, phone: Option[String])

val schema =
  StructType(
    Seq(
      StructField("name", StringType, nullable  = false),
      StructField("age", IntegerType, nullable = false),
      StructField("phone", StringType, nullable = true),
    )
  )
  
val ds Dataset[Person] = SparkSession.read.schema(schema).csv("path").as[Person].getOrThrow
```

ZIO-Spark, using magnolia, allows you to derive the StructType from your case class automatically:

```scala
import zio.spark.sql._

case class Person(name: String, age: Int, phone: Option[String])

val ds Dataset[Person] = SparkSession.read.schema[Person].csv("path").as[Person].getOrThrow
```