---
sidebar_position: 3
---

# TryAnalysis

Some Dataset transformations can throw `AnalysisException` making them not pure.

As an exemple if you want to filter a Dataset using a SQL expression such as:

```scala
df.filter("yolo")
```

It will throws an Exception even if we don't apply any action on the Dataset. These kind of transformation are not returning a `Dataset[T]` with spark-zio but a `TryAnalysis[Dataset[T]]`. a `TryAnalysis[T]` can be seen as a specific `Try[T]` meaning that you will have to handle the case of Failure.

You still can chain these kind of unsafe transformations and let Spark throws an exception using the following implicit:

```scala
import zio.spark.sql.TryAnalysis.syntax.throwAnalysisException
```

We still advice you handle the AnalysisException by throwing a domain specific error or recovering.
