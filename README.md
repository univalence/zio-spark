<h1 align="center">zio-spark</h1>

<p align="center">
  <a href="https://img.shields.io/badge/Project%20Stage-Development-yellowgreen.svg">
    <img src="https://img.shields.io/badge/Project%20Stage-Development-yellowgreen.svg" />
  </a>
  <a href="https://github.com/univalence/zio-spark/actions">
    <img src="https://github.com/univalence/zio-spark/actions/workflows/ci.yml/badge.svg" />
  </a>
  <a href="https://codecov.io/gh/univalence/zio-spark">
    <img src="https://codecov.io/gh/univalence/zio-spark/branch/master/graph/badge.svg" />
  </a>
  <a href="https://scala-steward.org">
    <img src="https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=" />
  </a>
</p>

<p align="center">
   A functional wrapper around Spark to make it work with ZIO
</p>

## Documentation

You can find the documentation of zio-spark [here](https://univalence.github.io/zio-spark/).


## Help

You can ask us (Dylan, Jonathan) for some help if you want to use the lib or have questions around it : https://calendly.com/zio-spark/help

## Latest version

If you want to get the very last version of this library you can still download it using:

```scala
libraryDependencies += "io.univalence" %% "zio-spark" % "0.0.1"
```

### Spark Version

zio-spark is built against spark 2.4.x and scala 2.12. Spark is provided, you should your Spark version (as you would
usually).

```scala
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7" % Provided
```

## Why ?

There are many reasons why we decide to build this library, such as:
* allowing user to build Spark pipeline with ZIO easily.
* making better code, pure FP, more composable, more readable Spark code.
* stopping the propagation of ```implicit SparkSessions```.
* improving some performances.
* taking advantage of ZIO allowing our jobs to retry and to be run in parallel.

## Contributions

Pull requests are welcomed. We are open to organize pair-programming session to tackle improvements. If you want to add
new things in `zio-spark`, don't hesitate to open an issue!

You can also talk to us directly using this link if you are interested to

## License

Copyright 2019-2022 Univalence.io

Licensed under the Apache License, Version 2.0:
http://www.apache.org/licenses/LICENSE-2.0
