package com.johnsnowlabs.utils.models

import org.apache.spark.ml.param.{Param, Params}

trait ResourceConsumerParams extends Params {
  protected final val modelResolver: Param[String] = new Param[String](this, "Something", "Something")

  protected final val rModelName: Param[String] = new Param[String](this, "Something", "Something")

  protected final val rModelVersion: Param[String] = new Param[String](this, "Something", "Something")

  protected final val rModelType: Param[String] = new Param[String](this, "Something", "Something")

  final def setModelResolver(s: String): this.type = set(modelResolver, s)

  final def setRModelName(s: String): this.type = set(rModelName, s)

  final def setRModelVersion(s: String): this.type = set(rModelVersion, s)

  final def setRModelType(s: String): this.type = set(rModelType, s)
}
