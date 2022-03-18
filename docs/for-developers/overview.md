---
sidebar_position: 1
---

# Overview

Most of the Zio-Spark code is auto generated, generally speaking it is just a wrapper around some spark classes with 
additional zio-spark specific functions to make it more ergonomics, safe and ZIO friendly.

Zio-Spark code is divided into two projects:
 - zio-spark-core -> The library containing the code that should be used by our zio-spark users
 - zio-spark-codegen -> The SBT plugin generating spark classes

We have two main goals, keep it simple for the users and respect the spark hierarchy. Someone that already using Spark 
should be able to use Zio-Spark with ease.