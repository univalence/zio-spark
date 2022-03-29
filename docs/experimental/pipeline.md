---
sidebar_position: 2
---

# Pipeline

**Pipelines** help you to structure your Spark jobs with ease.

If you are building simple pipelines using Dataset, we can see a pipeline as:

- a function reading a Dataset using a SparkSession.
- some transformations to obtain our output Dataset.
- an action to output the result from the transformed Dataset.

So to build a pipeline you will need to provide three functions describing the three steps above.

```scala
import zio.spark.sql._

val pipeline = Pipeline(
    load      = SparkSession.read.inferSchema.withHeader.withDelimiter(";").csv("test.csv"),
    transform = _.withColumn("new", $"old" + 1).limit(2),
    action    = _.count()
)
```

It creates a description of our job, you can then transform it into a ZIO effect using `run`:

```scala
val job: Spark[Long] = pipeline.run
```

`Spark[Long]` is an alias for `ZIO[SparkSession, Throwable, Long]` meaning that it returns an effect that need a 
SparkSession and will return a Long (the number of rows of the DataFrame).
