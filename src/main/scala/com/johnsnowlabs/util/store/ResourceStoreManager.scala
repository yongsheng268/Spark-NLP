package com.johnsnowlabs.util.store

import java.nio.file.{Files, Paths}

import com.johnsnowlabs.util.AnnotatorResource
import com.johnsnowlabs.util.store.common.StoredResource

object ResourceStoreManager {
  private var _storeFolderPath: String = Paths.get(System.getProperty("user.home"), "spark-nlp-store").toAbsolutePath.toString

  def getStoreFolderPath: String = _storeFolderPath

  def setStoreFolderPath(path: String) = { _storeFolderPath = path }

  def createStoreFolder: Unit = {
    Files.createDirectory(Paths.get(getStoreFolderPath))
  }
  def createOrReplaceResource[A](resource: AnnotatorResource[A], content: Array[Byte]): StoredResource[A] = {
    StoredResource[A]("path")
  }
}
