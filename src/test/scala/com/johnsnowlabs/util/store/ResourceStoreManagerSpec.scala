package com.johnsnowlabs.util.store

import java.io.{File, FileInputStream, FileReader}
import java.nio.file.{Files, Paths}

import com.johnsnowlabs.util.AnnotatorCorpus
import com.johnsnowlabs.util.resolvers.commons.SemVer
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import org.apache.commons.io.FileUtils
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.read

import scala.language.reflectiveCalls
import scalaj.http.Http

class ResourceStoreManagerSpec extends FlatSpec with Matchers with BeforeAndAfterEach {

  implicit val formats = DefaultFormats

  var tempStoreFolderPath: String = Paths.get(System.getProperty("user.home"), "spark-nlp-store-SDFGHJK").toAbsolutePath.toString

  def fixture() = new {
    val annotatorCorpus = AnnotatorCorpus("corpus-name", "pos", SemVer("1.0.0"))
    val corpusFile = Http("https://s3.amazonaws.com/auxdata.johnsnowlabs.com/spark-nlp-resources/corpus/spell/words.txt").asBytes.body
  }
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
    val f = fixture()
    val res = ResourceStoreManager.createOrReplaceResource(f.annotatorCorpus, f.corpusFile)
    Files.exists(Paths.get(res.path)) shouldBe true
    Files.exists(Paths.get(res.path, ResourceStoreManager.rawCorpusFile)) shouldBe true
    Files.exists(Paths.get(res.path, ResourceStoreManager.corpusMetadataFile)) shouldBe true
  }

  it should "save corpus with the correct content" in {
    val f = fixture()
    val res = ResourceStoreManager.createOrReplaceResource(f.annotatorCorpus, f.corpusFile)
    val f1 = new FileInputStream(Paths.get(res.path, ResourceStoreManager.rawCorpusFile).toString)
    val storedBytes = for { i <- 0 until f1.available() } yield f1.read.asInstanceOf[Byte]
    storedBytes shouldBe f.corpusFile.toSeq
  }

  it should "save AnnotatorCorpus metadata with the same values of the AnnotatorCorpus" in {
    val f = fixture()
    val res = ResourceStoreManager.createOrReplaceResource(f.annotatorCorpus, f.corpusFile)
    val f1 = FileUtils.readFileToString(new File(Paths.get(res.path, ResourceStoreManager.corpusMetadataFile).toString))
    val jsonAnnotatorCorpus = read[ResourceStoreManager.JsonAnnotatorCorpus](f1)
    jsonAnnotatorCorpus.corpusName shouldBe f.annotatorCorpus.name
    jsonAnnotatorCorpus.corpusType shouldBe f.annotatorCorpus.corpusType
    jsonAnnotatorCorpus.corpusVersion shouldBe f.annotatorCorpus.version.toString
  }
}
