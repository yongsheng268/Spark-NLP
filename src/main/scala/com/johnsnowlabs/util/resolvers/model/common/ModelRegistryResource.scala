package com.johnsnowlabs.util.resolvers.model.common

import com.johnsnowlabs.util.AnnotatorOnlineModel
import com.johnsnowlabs.util.resolvers.commons.SemVer
import org.apache.spark.SPARK_VERSION
import com.johnsnowlabs.sparknlp.SPARK_NLP_VERSION

case class ModelRegistryResource(
                                  modelName: String,
                                  modelType: String,
                                  modelVersion: SemVer,
                                  modelUri: String = "",
                                  sparkVersion: SemVer = SemVer(SPARK_VERSION),
                                  sparkNlpVersion: SemVer = SemVer(SPARK_NLP_VERSION),
                                  lang: String = "en"

                                ) {
}

object ModelRegistryResource {
  def apply(model: AnnotatorOnlineModel) = new ModelRegistryResource(
    model.name,
    model.modelType,
    model.version
  )
}