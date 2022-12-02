---
slug: /
sidebar_position: 1
---

# Introduction

Zio-spark provides an easy way to deal with Spark using ZIO runtime. The library is designed to be as close to the Spark 
API as possible. It allows us to use our favorite ZIO functionalities in our Spark job such as:

- using ZLayer to inject the SparkSession
- running jobs in parallel
- retrying jobs
- scheduling jobs
- canceling jobs
- increasing the performances of our jobs

## About performances

There are usual ways to improve the performances of spark jobs, in order of priority:

- less join
- less data (=> active location, streaming, ...)
- less udf/rdd
- better configuration
- better resource allocation 👈 ZIO Spark focus

What zio-spark can do is to launch different spark jobs in the same `SparkSession`, allowing to use more of the
executor capacities. E.g. if you have 5 workers, and only 1 is working to finish the current job, and you wait before
starting another job, it is not the best for the lead time.

On some pipeline, concurrent job launch speed up the pipeline by 2-10 times. It's not "faster", it's just the overall
lead time (wall clock time) is better.

Spark is very good at optimizing the work on a single job, there is no issue with spark, but the imperative nature of
the API don't allow Spark to know for remaining jobs.
