---
slug: /faq
sidebar_position: 6
---

# FAQ

## Why method is returning a NoSuchMethodError exception ?

It means that you use a function from ZIO Spark that is not implemented by your current version. ZIO Spark is based
on the latest Spark version for its API. However, we still allow you to choose your own version of Spark. It means that
you may use function that should not access normally. We still are working to find a way to provide this information at
compile time.

For more information, you can check the [adr](./adrs/choose-versioning-process.md).