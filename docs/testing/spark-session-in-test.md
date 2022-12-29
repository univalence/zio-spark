---
sidebar_position: 2
---

# SparkSession in test

Spark needs a SparkSession to run the dataset jobs. ZIO Spark test provides helpers to
get this SparkSession for free.

## One file SparkSession

Any objects that implements `ZIOSparkSpecDefault` trait is a runnable spark test.
So to start writing tests we need to extend `ZIOSparkSpecDefault`, which requires a `Spec`:

```scala
import zio.test._
import zio.spark.test._
import zio.spark.sql.implicits._

object MySpecs extends ZIOSparkSpecDefault {
  override def sparkSpec =
    suite("ZIOSparkSpecDefault can run spark job without providing layer")(
      test("It can run Dataset job") {
        for {
          df    <- Dataset(1, 2, 3)
          count <- df.count
        } yield assertTrue(count == 3L)
      }
    )
}
```

:::info

It is exactly the same thing as [ZIOSpecDefault](https://zio.dev/reference/test/writing-our-first-test).
It just provides a custom SparkSession by default.

:::

## Multi files SparkSession

The issue with the above example is that it will try to run many SparkSession if you have
tests in different files. Sadly, it is not correct since it will try to start many spark 
clusters at the same time. Hopefully, you can bypass it using `SharedZIOSparkSpecDefault`.

It is the same kind of code, however it will ensure that all the jobs from the different files
are finished before closing a unique SparkSession:

```scala
import zio.test._
import zio.spark.test._
import zio.spark.sql.implicits._

object MySpecs extends SharedZIOSparkSpecDefault {
  override def spec =
    suite("SharedZIOSparkSpecDefault can run shared spark job without providing layer")(
      test("It can run Dataset job") {
        for {
          df    <- Dataset(1, 2, 3)
          count <- df.count
        } yield assertTrue(count == 3L)
      }
    )
}
```

## Overriding SparkSession configuration

By default, here is the SparkSession configuration:

```scala
import zio.spark._

val defaultSparkSession: SparkSession.Builder =
SparkSession.builder
  .master("local[*]")
  .config("spark.sql.shuffle.partitions", 1)
  .config("spark.ui.enabled", value = false)
```

You can override it simply as follows:

```scala
import zio.test._
import zio.spark.test._
import zio.spark.sql.implicits._

abstract class ZIOSparkSpec extends ZIOSparkSpecDefault {
  override def ss: SparkSession.Builder = super.ss.config("", "")
}
```

And uses it the same way :

```scala
import zio.test._
import zio.spark.test._
import zio.spark.sql.implicits._

object MySpecs extends ZIOSparkSpec {
  override def spec = ???
}
```