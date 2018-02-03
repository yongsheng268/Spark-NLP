package com.johnsnowlabs.utils.models

import com.johnsnowlabs.nlp.util.ConfigHelper
import java.nio.file.{Files, Paths}

abstract class ModelResolver {
  var registryFile: Option[RegistryFile] = None

  def getRegistryFile: RegistryFile = registryFile.get

  def getRegistry: Option[RegistryFile]

  def retrieve(onlineModel: OnlineModel): Option[DownloadedModel]

  def isCached: Boolean = true

  def createStore(dir: String): Unit = {
    if (Files.exists(Paths.get(dir))) {
      Files.createDirectory(Paths.get(dir))
    }
  }

  def createModelFileName(onlineModel: OnlineModel): String =
    Array[String](onlineModel.modelName, onlineModel.modelType, onlineModel.modelVersion.toString).mkString("_")

  def createStore(): Unit = createStore(getUserStore)

  def getUserStore: String = ConfigHelper.retrieve.getString("modelResolver.store")
}
