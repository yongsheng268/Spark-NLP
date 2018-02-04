package com.johnsnowlabs.util.store

import java.io.File
import java.nio.file.{Files, Paths}

import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.apache.commons.io.FileUtils

import scala.language.reflectiveCalls

class ResourceStoreManagerSpec extends FlatSpec with Matchers with BeforeAndAfterEach {

  var tempStoreFolderPath: String = Paths.get(System.getProperty("user.home"), "spark-nlp-store-SDFGHJK").toAbsolutePath.toString

  override def afterEach(): Unit = {
    FileUtils.deleteDirectory(new File(tempStoreFolderPath))
  }

  "A ResourceStoreManager" should "have a store folder" in {
    ResourceStoreManager.getStoreFolderPath should not be empty
  }

  it should "have default store folder" in {
    ResourceStoreManager.getStoreFolderPath shouldEqual Paths.get(System.getProperty("user.home"), "spark-nlp-store").toAbsolutePath.toString
  }

  it should "reset store folder" in {
    ResourceStoreManager.setStoreFolderPath(tempStoreFolderPath)
    ResourceStoreManager.getStoreFolderPath shouldEqual tempStoreFolderPath
  }

  it should "create store folder" in {
    ResourceStoreManager.setStoreFolderPath(tempStoreFolderPath)
    ResourceStoreManager.createStoreFolder
    Files.exists(Paths.get(tempStoreFolderPath)) shouldBe true
  }

  it should "save an AnnotatorResource in the store" in {

  }
}
