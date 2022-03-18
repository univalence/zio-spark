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

Let's take Dataset as an example !

Everything start with a GenerationPlan, the plan read sources from Spark and use them to generate the Zio-Spark sources.
We decided to store the generated file in `src` directly. It allows us to compare the differences using git and it is
clearer for people to understand what's happening.

1. We read the Spark code source of the Dataset using SBT.
2. We use Scalameta to generate the automatically generated code.
   2. We group all class methods by type and wrapping them.
   3. We need to specify class specific import and folks.
3. We compile the project to generate the class.

The generated code contains only one kind of code: the code automatically generated from Spark.

You can then use this code to generate an overlays. They allow us to add our scala version specific and non-specific
functions.

These overlays can be found in `zio-spark-core/src/it/scala...`.

For Dataset, you will find four overlays:
- `zio-spark-core/src/it/scala/DatasetOverlay.scala` -> The code shared by all scala versions
- `zio-spark-core/src/it/scala-2.XX/DatasetOverlaySpecific.scala` -> The code specific for all scala versions

For non-specific code, the name must be `{ClassName}Overlay` and for specific code, the name must be 
`{ClassName}OverlaySpecific`.

If you recompile adding these overlays, the function between `template:on` and `template:off` will be added to the
generated code.