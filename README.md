# Spark-ZIO

[![Circle CI](https://circleci.com/gh/univalence/zio-spark.svg?style=svg)](https://app.circleci.com/pipelines/github/univalence/zio-spark)
[![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)

Spark-ZIO allows access to Spark using ZIO's environment.

## WHY ?

There are 2 main reasons to use this kind of technique of library : 
* making better code, pure FP, more composable, more readable (by some degree) and stopping the propagation of ```implicit SparkSessions```.
* improving some performances.

### About the performance
There are usual ways to improve the performances of spark jobs, in order of priority:
* less join
* less data (=> active location, streaming, ...)
* less udf/rdd
* better configuration
* **better resource allocation** <-

What zio-spark can do is to launch different spark jobs in the same `SparkSession`, allowing to use more of the executors capacity. Eg. if you have 5 workers, and only 1 is working to finish the current job, and you wait before starting another job, that's not what's best efficiency, and at the end not the best for the lead time.

On some pipeline, concurrent job launch speed up the pipeline by 2-10 times.
It's not "faster", it's just the overall lead time (wall clock time) is better.

Spark is very good at optimizing the work on a single job, there is no issue with spark, but the imperative nature of the API don't allow Spark to know for remaining jobs.


More about that in this blog article (French):
* [Amélioration du lead time des chaines en spark](https://univalence.io/blog/articles/amelioration-du-lead-time-des-chaines-en-spark-avec-un-peu-de-monix/)


## Latest version

If you want to get the very last version of this library you can still download it using bintray here : https://bintray.com/univalence/univalence-jvm/spark-zio

```scala
libraryDependencies += "io.univalence" %% "zio-spark" % "0.0.1"
```

### Spark Version

zio-spark is built against spark 2.4.x and scala 2.12.
Spark is provided, you should your Spark version (as you would usually).

```scala
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7" % Provided
```

### Snapshots

```scala
resolvers += "univalence" at "http://dl.bintray.com/univalence/univalence-jvm"

libraryDependecies += "io.univalence" %% "zio-spark" % "0.0.1-+"
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

## Syntax

zio-spark uses the syntax from ZIO + a new wrapping of Spark existing types to make them pure.
```scala
class ZRDD[T](private val rdd: RDD) extends ZWrap(rdd) {
  def count:Task[Long] = execute(_.count())

  /* ... */
}
```

The provided type use the "execute pattern", which allow to tap into Spark types, and compose using new definition that are more type safe.

For the RDD, we have a ZRDD:
```scala

class ZRDD[T](rdd: RDD[T]) {

  //To access directly spark API
  def execute[B, Pure](f: RDD[T] => B)(implicit C: Clean.Aux[B, Pure]): Task[Pure]
  def executeM[R, B, Pure](f: RDD[T] => RIO[R, B])(implicit C: Clean.Aux[B, Pure]): RIO[R, Pure]


  //Rewrapped APIs, as close as possible to the original API, making them pure
  def ++(zrdd: ZRDD[T]): ZRDD[T]

  def ++(rdd: RDD[T]): ZRDD[T]

  def collect: Task[Seq[T]]

  def count: Task[Long]

  def flatMap[B: ClassTag](f: T => Seq[B]): ZRDD[B]

  def id: Int

  def map[B: ClassTag](f: T => B): ZRDD[B]

  def mapPartitions[B: ClassTag](f: (Iterator[T]) => Iterator[B]): ZRDD[B]

  def name: UIO[String]

  def saveAsTextFile(path: String): Task[Unit]

  def toDF(colNames: String*)(implicit encoder: Encoder[T]): RIO[SparkEnv, ZDataFrame]

  /* ... */
}
```

### Running jobs in parallel

You can run job in parallel as you would run things in parallel using ZIO.

```scala

object RunPar extends zio.App {

  import zio.spark.syntax._

  val prg: RIO[Console with SparkEnv, Unit] = for {
    df   <- zio.spark.sql("select * from person")
    _    <- df.cache
    df2  <- df.filter("age >= 18").toTask //filter return a Try[ZDataFrame]
    pair <- df.count zipPar df2.count
    _    <- zio.console.putStr(s"${pair._1} persons (${pair._2} adults)")
  } yield {}

  override def run(args: List[String]): URIO[zio.ZEnv, ExitCode] =
    prg.provideCustomLayer(zio.spark.builder.master("local").appName("counts").getOrCreate).exitCode

  //would work with anything with Par, like collectPar, ... and fibers
  {
    val prg: RIO[Console with SparkEnv, Unit] = for {
      df         <- zio.spark.sql("select * from person")
      _          <- df.cache
      df2        <- df.filter("age >= 18").toTask //filter return a Try[ZDataFrame]
      
      personsJob <- df.count.forkAs("persons")
      adultsJob  <- df2.count.forkAs("adults")
      persons    <- personsJob.join
      adults     <- adultsJob.join
      
      _          <- zio.console.putStr(s"$persons persons ($adults adults)")
    } yield {}
  }
}
```

## Contributions

Pull requests are welcomed. We are open to organize pair-programming session to tackle improvements.
If you want to add new things in zio-spark, don't hesitate to open an issue!


## Roadmap

 * Cancellable computations: find an non invasive way to cancel jobs as you would cancel ZIO computations
 * Externalize purity Auxs: the purity mechanism is not exclusive to zio-spark. It should be externalized and reworked

## Alternatives

 *  (Deprecated) [spark-zio 0.3](https://github.com/univalence/spark-tools/tree/master/spark-zio), our first experiment to combine ZIO and Spark
 *  [ZparkIO](https://github.com/leobenkel/ZparkIO) a framework for Spark, ZIO

## License

Copyright 2019 Univalence.io

Licensed under the Apache License, Version 2.0:
http://www.apache.org/licenses/LICENSE-2.0
