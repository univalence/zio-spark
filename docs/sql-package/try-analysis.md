---
sidebar_position: 3
---

# TryAnalysis

Some Dataset transformations can throw `AnalysisException` making them not pure.

As an example if you want to filter a Dataset using a SQL expression such as:

```scala
val newDf: TryAnalysis[DataFrame] = df.filter("yolo")
```

It will throw an Exception even if we don't apply any action on the Dataset. These kinds of transformation are not 
returning a `Dataset[T]` but a `TryAnalysis[Dataset[T]]`. a `TryAnalysis[T]` can be seen as a specific 
`Try[T]` that only throws an AnalysisException meaning that you will have to handle the case of Failure.

To handle the possible error, you can recover from a TryAnalysis:

```scala
val newDf: DataFrame = df.filter("yolo").recover(_ => df)
```

You also can accumulate TryAnalysis transformations and recover from them at once:

```scala
val newDf: DataFrame = df.filter("yolo").as[NotAMatchingClass].recover(_ => df)
```

You still can chain these unsafe transformations and let Spark throws an exception at run-time using the following 
implicit:

```scala
import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException

val newDf: DataFrame =  df.filter("yolo")
```

We still advice you handle the AnalysisException by throwing a domain specific error or recovering.
