package com.johnsnowlabs.util.store

import java.io.{File, FileOutputStream, PrintWriter}
import java.nio.file.{FileAlreadyExistsException, Files, Paths}

import com.johnsnowlabs.util.resolvers.commons.SemVer
import com.johnsnowlabs.util.{AnnotatorCorpus, AnnotatorResource}
import com.johnsnowlabs.util.store.common.StoredResource
import org.apache.commons.io.FileUtils
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.write

object ResourceStoreManager {
  implicit val formats = DefaultFormats

  val rawCorpusFile: String = "corpus"
  val corpusMetadataFile: String = "metadata"

  private var _storeFolderPath: String = Paths.get(System.getProperty("user.home"), "spark-nlp-store").toAbsolutePath.toString

  case class JsonAnnotatorCorpus(corpusName: String, corpusType: String, corpusVersion: String)

  def getStoreFolderPath: String = _storeFolderPath

  def setStoreFolderPath(path: String) = { _storeFolderPath = path }

  def createStoreFolder: Unit = {
    try {
      Files.createDirectory(Paths.get(this.getStoreFolderPath))
    } catch {
      case _: FileAlreadyExistsException =>
      case e: Throwable => e.printStackTrace()
    }
  }

  def initialSetup: Unit = {
    createStoreFolder
  }

  def createOrReplaceResource(corpus: AnnotatorCorpus, content: Array[Byte]): StoredResource[AnnotatorCorpus] = {
    this.initialSetup

    val folderNameForResource: String = this.folderNameForResource(corpus)
    try {
      FileUtils.deleteDirectory(new File(folderNameForResource))
      Files.createDirectory(Paths.get(folderNameForResource))
      this.saveResourceContent(Paths.get(folderNameForResource, rawCorpusFile).toString, content)
      this.saveResourceMetadata(Paths.get(folderNameForResource, corpusMetadataFile).toString, corpus)
    } catch {
      case e: Throwable => e.printStackTrace()
    }
    StoredResource[AnnotatorCorpus](folderNameForResource, corpus)
  }

  def folderNameForResource(corpus: AnnotatorCorpus): String =
    Paths.get(this.getStoreFolderPath, corpus.stringId).toAbsolutePath.toString

  def saveResourceContent(str: String, bytes: Array[Byte]) = {
    val outStream = new FileOutputStream(str)
    outStream.write(bytes)
    outStream.close()
  }

  def saveResourceMetadata(str: String, corpus: AnnotatorCorpus): Unit = {
    val outWriter = new PrintWriter(str)
    val jsonAnnotatorCorpus = JsonAnnotatorCorpus(corpus.name, corpus.corpusType, corpus.version.toString)
    outWriter.write(write(jsonAnnotatorCorpus))
    outWriter.close()
  }
}
