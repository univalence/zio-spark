---
slug: /faq
sidebar_position: 6
---

# FAQ

## Why method is returning a NoSuchMethodError exception ?

It means that you use a function from ZIO Spark that is not implemented by your current version. ZIO Spark is based
on the latest Spark version for its API. However, we still allow you to choose your own version of Spark. It means that
you may use function that should not access normally. We still are working to find a way to provide this information at
compile time.

For more information, you can check the [adr](./adrs/choose-versioning-process.md).


## What is the difference with Zparkio ?

A lot of difference. 
While the initial pattern is the same 
(use ZIO to have concurrent launches of Spark jobs, in order to maximise the use of a Spark cluster for small jobs), 
the two projects differ.

* zio-spark, in this version, provide an exhaustive thin layer on top of the existing API (SparkSession, RDD, Dataset, ...).
  * Zparkio is not providing a lot of ops, and do them in a chaining style. (`ZIO[_, _, DS[T]].$method$ `)
* Zparkio is providing a complete scaffolding for logs, args, factories.
  * zio-spark is not providing anything in that area, we are working on example to show how you could use zio-cli, zio-config, ...
* zio-spark is solving a lot of core issues with the use of Spark:
  * implicits and encoders, you don't need to pass the sparkSession around, you just need to <br />`import zio.spark.sql.implicits._`.
  * better error management, especially for `AnalysisException`.
  * `CancellableJob` (experimental), you can stop running spark jobs directly from ZIO.
  * `MapWithEffect` (experimental), you can call effects during data transformation, for example calling an API (without the fear of internal DDoS) (scala 2.13 only at the moment).



Also, we think the design of zio-spark is better to work existing spark projects:
* It's just a lib, you can integrate it partially.
* There are tools to work with exiting code like:
  * `zio.spark.fromSpark(SparkSession => T)`
  * `.zioSpark` to convert a type from `org.apache.spark` to `zio.spark`.

That been said, both project are easy to try, just launch some giter8, and see what fits you.
