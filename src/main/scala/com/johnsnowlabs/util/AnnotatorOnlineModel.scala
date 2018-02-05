package com.johnsnowlabs.util

import com.johnsnowlabs.util.resolvers.commons.SemVer
import org.apache.spark.SPARK_VERSION
import com.johnsnowlabs.sparknlp.SPARK_NLP_VERSION

class AnnotatorOnlineModel(
                          var _name: String = "",
                          var _modelType: String = "",
                          var _version: SemVer = SemVer("0.0.0"),
                          var sparkVersion: String = SPARK_VERSION,
                          var sparkNlpVersion: String = SPARK_NLP_VERSION,
                          var lang: String = "en"
                          ) extends AnnotatorResource[AnnotatorOnlineModel] {
  def setModelType(t: String): this.type = {
    _modelType = t
    this
  }

  def modelType: String = _modelType

  def stringId: String = s"${name}-${version}-${modelType}-spark_${sparkVersion}-sparknlp_${sparkNlpVersion}-${lang}"
}

object AnnotatorOnlineModel {
  def apply(name: String, modelType: String, ver: SemVer): AnnotatorOnlineModel = {
    new AnnotatorOnlineModel(name, modelType, ver)
  }
}