---
sidebar_position: 2
---

# Getting started

## Installation

:warning: The library is currently under a huge refactoring, we will make a release soon with a lot of breaking changes.

zio-spark is, for the moment, composed by only one core library regrouping the sql and core spark libraries.

If you are using sbt, just add the following line to your `build.sbt`:

```scala
libraryDependencies += "io.univalence" %% "zio-spark" % "0.13.0"
```

Spark version is provided. It means that you have to provide your own Spark version (as you would usually).

## Examples

To start with zio-spark, we advise you to look at the 
[examples](https://github.com/univalence/zio-spark/tree/master/examples/src/main/scala).
