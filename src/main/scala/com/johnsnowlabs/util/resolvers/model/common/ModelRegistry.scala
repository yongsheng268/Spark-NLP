package com.johnsnowlabs.util.resolvers.model.common

import com.johnsnowlabs.util.AnnotatorOnlineModel

case class ModelRegistry(modelList: Seq[ModelRegistryResource]) {
  def length: Int = this.modelList.length

  def size = length

  def hasModel(model: ModelRegistryResource): Boolean = {
    modelList.exists {
      case ModelRegistryResource(modelName, modelType, modelVersion, _, sparkVersion, sparkNlpVersion, lang) =>
        model.modelName == modelName && model.modelType == modelType && model.modelVersion == modelVersion && model.sparkVersion.major == sparkVersion.major && model.sparkNlpVersion == sparkNlpVersion && model.lang == lang
    }
  }

  def hasModel(model: AnnotatorOnlineModel): Boolean = {
    hasModel(ModelRegistryResource(model))
  }

  def findModel(model: ModelRegistryResource): ModelRegistryResource = {
    modelList.find {
      case ModelRegistryResource(modelName, modelType, modelVersion, _, sparkVersion, sparkNlpVersion, lang) =>
        model.modelName == modelName && model.modelType == modelType && model.modelVersion == modelVersion && model.sparkVersion.major == sparkVersion.major && model.sparkNlpVersion == sparkNlpVersion && model.lang == lang
    }.get
  }

  def findModel(model: AnnotatorOnlineModel): ModelRegistryResource = {
    this.findModel(ModelRegistryResource(model))
  }

}
