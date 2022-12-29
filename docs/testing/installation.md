---
sidebar_position: 1
---

# Installing ZIO Spark Test

In order to use ZIO Spark Test, we need to add the required configuration in our SBT settings:

```scala
libraryDependencies ++= Seq(
  "io.univalence" %% "zio-spark-test" % "0.10.0+00035-07d67985-SNAPSHOT" % Test,
)

testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework")
```

:::caution

For the moment, we only provide a snapshot version, it is still experimental. You can see the latest 
version on [nexus](https://oss.sonatype.org/#nexus-search;gav~io.univalence~zio-spark-test_2.13~~~).

:::
