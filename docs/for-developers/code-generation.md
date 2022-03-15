---
sidebar_position: 2
---

# Code Generation

Like we already said, the main Spark classes are auto generated in Zio-Spark. We have to add our own classes because
Spark is Impure and not ZIO aware.

A generated class is composed by three kind of codes:
- The code automatically generated from Spark
- The scala version specific code written in Zio-Spark to add Zio-Spark functionalities
- The scala version non-specific code written in Zio-Spark to add Zio-Spark functionalities

## Example

Generally speaking here is the workflow that auto-generate a Zio-Spark class:

Let's take `Dataset` as an example !

Everything start with a GenerationPlan, the plan read sources from Spark and use them to generate the `Zio-Spark` sources.
We decided to store the generated file in `zio-spark-core/src/main/scala-$version/zio/spark/sql/Dataset.scala` directly. 
It allows us to compare the differences using git and it is clearer for people to understand what's happening.

When SBT is compiling the `core` module, based on the `zio-spark-codegen` plugin :

1. We read the Spark code source of the `Dataset` using SBT 
   (in `org.apache.spark/spark-sql:org/apache/spark/sql/Dataset.scala`)
2. We use Scalameta to read the source, analyse, and generate the code for zio-spark
   * We group all class methods by type and wrapping them.
   * We need to specify class specific import and folks.
   * We add extra chunks of code from the overlays
3. We write the merged output for the target scala version (current `$version` used by the core module during 
  compilation) into  `zio-spark-core/src/main/scala-$version/zio/spark/sql/Dataset.scala`

### Overlays

At the begining, the generated code contains only one kind of code: the code automatically generated from Spark.

You can then use this code to generate an overlays. They allow us to add our scala version specific and non-specific
functions.

These overlays can be found in `zio-spark-core/src/it/scala...`.

For Dataset, you will find four overlays:
- `zio-spark-core/src/it/scala/DatasetOverlay_$SUFFIX.scala` -> The code shared by all scala versions
- `zio-spark-core/src/it/scala-$version/DatasetOverlay_$SUFFIX.scala` -> The code specific for all scala versions

A `$SUFFIX` can be used to split the code into different parts and avoid collision between a scala version specific 
overlay and a non version specific (it wouldn't stop the code generation from working, however it will break the 
compilation checks on `src/main/it`).

For general name pattern is `${ClassName}Overlay_${SUFFIX}` (`$ClassName` in `Dataset`, `RDD`, ...)

If you recompile adding these overlays, the function between `template:on` and `template:off` will be added to the
generated code.
