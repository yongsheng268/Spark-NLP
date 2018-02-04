package com.johnsnowlabs.utils.models

import com.johnsnowlabs.util.resolvers.commons.SemVer

case class RegistryModel(
                          modelName: String,
                          modelType: String,
                          modelVersion: SemVer,
                          modelUri: String
                        )

case class RegistryFile(
                       modelList: Seq[RegistryModel]
                       ) {
  def length: Int = this.modelList.length

  def size = length

  def hasModel(onlineModel: OnlineModel): Boolean = {
    modelList.exists {
      case RegistryModel(modelName, modelType, modelVersion, _) =>
        onlineModel.modelName == modelName && onlineModel.modelType == modelType && onlineModel.modelVersion == modelVersion
    }
  }

  def findModel(onlineModel: OnlineModel): RegistryModel = {
    modelList.find {
      case RegistryModel(modelName, modelType, modelVersion, _) =>
        onlineModel.modelName == modelName && onlineModel.modelType == modelType && onlineModel.modelVersion == modelVersion
    }.get
  }
}
