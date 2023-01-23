---
sidebar_position: 3
---

# ZIO Spark Test assertions

ZIO Spark Test gives you a unique way of asserting the content of a Dataframe.

To start with this assertion you just have to import:

```scala
import zio.spark.test._
```

It provides a DSL that allow you to write the following kind of assertion:

```scala
import zio.test._
import zio.spark._
import zio.spark.test._

import org.apache.spark.sql.types.{IntegerType, StringType}

test("...") {
  for {
    df     <- ??? // DataFrame of Person(name: String, age: Int)
    result <- df.expectAll(
      schema("name".as(StringType), "age".as(IntegerType)),
      row("Joanna", 25),
      row("Paul", 48),
    )
  } yield result
}
```

The function `expectAll` allows you to provide an optional schema matcher and row matchers.

## Schema matcher

The schema matcher is an optional matcher that should be in first position.

It has two purposes:
 - It allows you to select a small parts of the column for your assertions
 - It allows you to validate a schema based on the expected column names and types

Here is an example of schema matcher asking for two columns :

```scala
import zio.spark.test._
import org.apache.spark.sql.types.{IntegerType, StringType}

val matcher = schema("name".as(StringType), "age".as(IntegerType))
```

## Row matcher

The row matcher is a rule that should match a row. You can build a row matcher in several ways.

### Positional row matcher

A positional row matcher is a matcher that will try to match all row values one by one.

As an example, if you have a DataFrame composed by a name (String) and an age (Int). You can
provide the following row matcher:

```scala
import zio.spark.test._

val matcher = row("John", 18)
```

This matcher will exactly match a Row containing a person named John who are 18. Sometimes, you
only want to match one of the column. You have two choices, like we said before, we can specify
the schema used for the assertion such as:

```scala
import zio.test._
import zio.spark._
import zio.spark.test._

import org.apache.spark.sql.types.IntegerType

test("...") {
  for {
    df     <- ??? // DataFrame of Person(name: String, age: Int)
    result <- df.expectAll(
      schema("age".as(IntegerType)),
      row(18)
    )
  } yield result
}
```

Or we can use `__` to match on any value for a particular cell such as:

```scala
import zio.test._
import zio.spark._
import zio.spark.test._

test("...") {
  for {
    df     <- ??? // DataFrame of Person(name: String, age: Int)
    result <- df.expectAll(row(__, 18))
  } yield result
}
```

Sometimes, for a particular cell, you don't want to match on a particular value but assert
that the value respect a predicate. You can do the same with the DSL:

```scala
import zio.test._
import zio.spark._
import zio.spark.test._

test("...") {
  val isAdult: Int => Boolean = _ > 18
  
  for {
    df     <- ??? // DataFrame of Person(name: String, age: Int)
    result <- df.expectAll(row(__, isAdult))
  } yield result
}
```

### Global row matcher

You also can build a global matcher using column name to verify a row. If we still want
to make an assertion about the age of a person we could create a matcher as follows:

```scala
import zio.spark.test._

val matcher = row("age" -> 18)
```

The argument should be a key and a value. The key is the name of the column and the value can be
any positional matcher seen before. Here is an example with a predicate:

```scala
import zio.spark.test._

val isAdult: Int => Boolean = _ > 18
val matcher = row("age" -> isAdult)
```

### Match multiple rows

As we said before in normal mode, each rows should have its own matcher using `expectAll`.
However, it can be useful to have one matcher matching more than one rows for data validation
purpose. To allow a matcher to match multiple rows, you just have to turn the feature on:

```scala
import zio.spark.test._

val isAdult: Int => Boolean = _ > 18
val matcher = row("age" -> isAdult).allowMultipleMatches
```


