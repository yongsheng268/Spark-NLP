package com.johnsnowlabs.utils.models

import scala.collection.mutable

object ModelDownloader {
  type ResolverName = String

  val jslResolver = new JslModelResolver

  val modelDownloaders: mutable.Map[ResolverName, ModelResolver] = mutable.Map[ResolverName, ModelResolver](
    "jsl" -> jslResolver,
    "default" -> jslResolver
  )

  /*
  * Main entry to point for clients to resolver models
  *
  */
  def resolveModelWith(resolverName: ResolverName, onlineModel: OnlineModel) : DownloadedModel = {
    require(modelDownloaders.contains(resolverName), s"Resolver named $resolverName doesn't exist")

    val resolver: ModelResolver = modelDownloaders(resolverName)
    resolver.retrieve(onlineModel).get
  }

  def resolveModelWith(resolverName: ResolverName, modelName: String, modelVersion: String, modelType: String) : DownloadedModel = {
    resolveModelWith(resolverName, OnlineModel(modelName, ModelVersion(modelVersion), modelType))
  }
}
