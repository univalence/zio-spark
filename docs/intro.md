---
slug: /
sidebar_position: 1
---

# Introduction

There are usual ways to improve the performances of spark jobs, in order of priority:

* less join
* less data (=> active location, streaming, ...)
* less udf/rdd
* better configuration
* **better resource allocation** <-

What zio-spark can do is to launch different spark jobs in the same `SparkSession`, allowing to use more of the
executors capacity. Eg. if you have 5 workers, and only 1 is working to finish the current job, and you wait before
starting another job, that's not what's best efficiency, and at the end not the best for the lead time.

On some pipeline, concurrent job launch speed up the pipeline by 2-10 times. It's not "faster", it's just the overall
lead time (wall clock time) is better.

Spark is very good at optimizing the work on a single job, there is no issue with spark, but the imperative nature of
the API don't allow Spark to know for remaining jobs.
