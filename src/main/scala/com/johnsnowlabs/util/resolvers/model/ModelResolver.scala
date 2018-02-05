package com.johnsnowlabs.util.resolvers.model

import com.johnsnowlabs.util.AnnotatorOnlineModel
import com.johnsnowlabs.util.resolvers.model.common.{ModelRegistry, ModelRegistryResource}

trait ModelResolver {
  def getRegistry: Option[ModelRegistry]
  def getModel(model: ModelRegistryResource): Option[Array[Byte]]
  def getModel(model: AnnotatorOnlineModel): Option[Array[Byte]]
}