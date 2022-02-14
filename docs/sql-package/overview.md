---
sidebar_position: 1
---

# Overview

The sql package provides all the code to interact with Dataset and DataFrame using zio-spark.

It contains essentialy, the wrapper around the DataFrameReader, DataFrameWriter, SparkSession and Dataset. Since we tried to be as close as possible to the Spark API, it is not usefull to write documentations about the Spark wrappers, we advice you to look at [Spark documentation](https://spark.apache.org/docs/latest/).

However it also contains some zio-spark specific features, that can help you build better pipelines and are documented here.
