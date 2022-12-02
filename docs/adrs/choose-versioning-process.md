# Versioning process choice

* Status: accepted
* Deciders:
    * Jonathan Winandy
    * Dylan Do Amaral

Technical Story: We need to find a way to allow people to use their own Spark version with ZIO Spark.

## Context and Problem Statement

Generally, people in production can't choose their Spark version freely. We can't force them, with ZIO Spark, to use
the latest.

## Decision Drivers

* Simplicity
* Stability

## Considered Options

* Use a version matrix
* Force the Spark version
* Make the Spark version provided

## Decision Outcome

Chosen option: "Make the Spark version provided", because it is the less cumbersome way we know for end users.

### Positive Consequences

* It allows end users to choose their Spark version

### Negative Consequences

* It can provoke NoSuchMethodError exception if the user use a method that is not provided by its Spark version.

## Pros and Cons of the other Options

### Use a version matrix

We thought to use a version matrix such as [Elastic4s](https://github.com/sksamuel/elastic4s#release) matrix. However,
it would make de code harder to maintain and people could not choose the patch version that they want?

### Force the Spark version

It was immediately rejected since end user have to deal with a particular version of Spark all the time.

## Notes

* We are still looking for a better alternative with no downside.