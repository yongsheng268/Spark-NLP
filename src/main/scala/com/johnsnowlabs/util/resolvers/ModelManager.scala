package com.johnsnowlabs.util.resolvers

import com.johnsnowlabs.util.AnnotatorOnlineModel
import com.johnsnowlabs.util.resolvers.manager.BaseManager
import com.johnsnowlabs.util.resolvers.model.common.ModelRegistry
import com.johnsnowlabs.util.resolvers.model.{HttpModelResolver, ModelResolver}
import com.johnsnowlabs.util.store.ResourceStoreManager
import com.johnsnowlabs.util.store.common.StoredResource

import scala.collection.mutable

object ModelManager extends BaseManager {
  type ModelResolverName = String

  var resolverName: ModelResolverName = "default"

  var resolvers: mutable.Map[ModelResolverName, ModelResolver] = mutable.Map[ModelResolverName, ModelResolver](
    "default" -> HttpModelResolver()
  )

  def getResolver: ModelResolver = resolvers(resolverName)


  def retrieve(modelName: String, modelType: String, modelVersion: String): DownloadedResource[AnnotatorOnlineModel] =
    retrieve(AnnotatorOnlineModel(modelName, modelType, modelVersion))

  def retrieve(model: AnnotatorOnlineModel): DownloadedResource[AnnotatorOnlineModel] = {
    if (this.isModelCached(model)) {
      this.retrieveFromCache(model)
    } else {
      this.retrieveFromRepo(model)
    }
  }

  def isModelCached(model: AnnotatorOnlineModel): Boolean = {
    false
  }

  def retrieveFromCache(model: AnnotatorOnlineModel): DownloadedResource[AnnotatorOnlineModel] = {
    DownloadedResource[AnnotatorOnlineModel]("", model)
  }

  def retrieveFromRepo(model: AnnotatorOnlineModel): DownloadedResource[AnnotatorOnlineModel] = {
    val resolver = getResolver
    val modelRegistry: ModelRegistry = resolver.getRegistry.get

    if (modelRegistry.hasModel(model)) {
      val body: Option[Array[Byte]] = resolver.getModel(modelRegistry.findModel(model))
      val storedResource: StoredResource[AnnotatorOnlineModel] = ResourceStoreManager.createOrReplaceResource(model, body.get)
      DownloadedResource(storedResource.path, model)
    } else {
      throw new Exception("File not found")
    }
  }

}
