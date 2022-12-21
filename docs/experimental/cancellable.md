---
sidebar_position: 3
---

# Cancellable Effects

**zio.spark.experimental.CancellableEffect** helps you to stop spark jobs.

To this point, it's very difficult to programmatically stop spark jobs, because we don't have a clear handler on the execution.
```scala
val df:org.apache.spark.sql.Dataframe = ...

val res:Long = df.count() // No handler
```

There are some solution in Spark using `job.group.id` or other things to stop a group of job or a job.

_We could take some effort to have a better handler on each job, however it may create uncontrolled 
stability in the library._

Instead, we have a wrapper, using the `zio.Executor` from ZIO to make some jobs cancellable : 
```scala
import zio.spark.experimental.CancellableEffect

val df:zio.spark.sql.Dataset = ...

val getRes:RIO[zio.spark.sql.SparkSession, Long] = CancellableEffect.makeItCancellable(df.count)

//...
// it will stop the job after 10 seconds
getRes.timeout(10.seconds)

// it will stop the slowest job (each run is independent)
getRes.raceWith(getRes)
```







