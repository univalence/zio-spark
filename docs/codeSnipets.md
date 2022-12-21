---
slug: /code-examples
sidebar_position: 7
---

# Code Examples


## Seq to Dataset

```scala

//don't need to import ss.implicits anymore
import zio.spark.sql.implicits._

//implicits on sequence
val createPersonDs: URIO[SparkSession, Dataset[Person]] = Seq(Person("toto", 13)).toDS

```

## Manage Error on Loads


```scala

//use of ZIO + effects composition to load a file or another if it doesn't work
val input: ZIO[SparkSession, Throwable, DataFrame] = SparkSession.read.csv("path1") orElse SparkSession.read.csv("path2")

```

## Manage Analysis Errors

Analysis erros, are error returned by Spark (`org.apache.spark.sql.AnalysisException`) when something wrong happen building
the data transformation. For example, if you select a field that do not exist, of it you try to cast `as` a type, for
the Dataframe schema doesn't match.


In zio-spark, those potential errors are isolated, you can either : 
* recover from them
* throws directly (as would apache spark do)

```scala
import zio.spark.sql.implicits._ //if not done already that the beginning of the file

//explicit error management + encoder on Person
def process1(input: DataFrame): Task[Long] = input.as[Person] match {
  case TryAnalysis.Failure(_) => input.filter("age >= 18").getOrThrow.count
  case TryAnalysis.Success(ds) => ds.count
}

//less error management
def process2(input: DataFrame) = {
  import zio.spark.sql.syntax.throwsAnalysisException._

  val selectPerson:Dataset[Person] = input.as[Person]
  selectPerson.filter(_.isAdult).count

}
```

## Cancellable Effects + Caching

Caching mecanisme is from Spark, it's effecful, you need to call it before doing a computation to mark the dataset as something
to be cached. CancellableEffect is an experimental feature from zio-spark, that allow to propagate an Interrupt to stop the job. 

```scala

//cache, cancellable
def improveProcess(input: DataFrame): ZIO[SparkSession, Throwable, Long] =
  for {
    cached <- input.cache
    p = CancellableEffect.makeItCancellable(process(cached))
    //if the first job is stale, it launches a second job. When on of the jobs finish, it stops the remaining job
    count <- p.race(p.delay(10.seconds))
  } yield count
```


```