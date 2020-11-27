# Spark-ZIO

[![Circle CI](https://circleci.com/gh/univalence/zio-spark.svg?style=svg)](https://app.circleci.com/pipelines/github/univalence/zio-spark)
[![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)

Spark-ZIO allows access to Spark using ZIO's environment.

## Latest version

If you want to get the very last version of this library you can still download it using bintray here : https://bintray.com/univalence/univalence-jvm/spark-zio

```scala
libraryDependencies += "io.univalence" %% "zio-spark" % "0.0.2"
```

### Snapshots

```scala
resolvers += "univalence" at "http://dl.bintray.com/univalence/univalence-jvm"

libraryDependecies += "io.univalence" %% "zio-spark" % "0.0.2-XXXX-XXXX"
```

## Migrating existing code

Migration using zio-spark should be easy, you can start to use the `retroCompat` method:

```scala

import zio._
import zio.spark._

val job1: RIO[SparkEnv, Unit] = retroCompat(ss => {
  //OLD CODE
  val df = ss.read.json("examples/src/main/resources/people.json")
  
  df.show()
  import ss.implicits._
  df.printSchema()
  df.select("name").show()
  df.select($"name", $"age" + 1).show()
  df.filter($"age" > 21).show()
  df.groupBy("age").count().show()
  df.createOrReplaceTempView("people")
  
  val sqlDF = ss.sql("SELECT * FROM people")
  sqlDF.show()
  df.createGlobalTempView("people")
  
  ss.sql("SELECT * FROM global_temp.people").show()
  ss.newSession().sql("SELECT * FROM global_temp.people").show()                    
})
```

You can use `retroCompat` to get the new datatypes:
```scala
import zio._
import zio.spark._

val ds1: RIO[SparkEnv, ZDataFrame] = retroCompat(ss => {
  ss.read.json("examples/src/resources/people.json").filter("age > 21")
})

val job2: RIO[SparkEnv, Unit] = ds1 >>= (_.printSchema)
```

##Syntax

zio-spark uses the syntax from ZIO + a new wrapping of Spark existing types to make them pure.
```scaclass ZRDD[T
](private val rdd:RDD) extends ZWrap(rdd){
  def count:Task[Long] = execute(_.count())



  /* ... */
}
```

All the existing type useute pattern, which allow to tap into Spark types, and compose using the new wrapping whou



## Roadmap

 * Cancellable computations: find an non invasive way to cancel jobs as you would cancel ZIO computations
 * Externalize purity Auxs: the purity mechanism is not exclusive to zio-spark. It should be externalized and reworked

## Alternative

 *  (Deprecated) [spark-zio 0.3](https://github.com/univalence/spark-tools/tree/master/spark-zio), our first experiment to combine ZIO and Spark
 *  [ZparkIO](https://github.com/leobenkel/ZparkIO) a framework for Spark, ZIO

## License

Copyright 2019 Univalence.io

Licensed under the Apache License, Version 2.0:
http://www.apache.org/licenses/LICENSE-2.0
