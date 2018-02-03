package com.johnsnowlabs.utils.models

import scalaj.http._
import org.json4s._
import org.json4s.jackson.JsonMethods.parse
import com.johnsnowlabs.nlp.util.ConfigHelper
import sys.process._
import java.net.URL
import java.io.File
import java.nio.file.Paths

import scala.reflect.io.Streamable.Bytes

case class JsonModel(modelName: String, modelVersion: String, modelUri: String)
case class JsonModelType(modelType: String, models: List[JsonModel])
case class JsonRegistry(regVersion: String, modelTypes: List[JsonModelType])

class JslModelResolver(registryRepo: String) extends ModelResolver {
  implicit val formats = DefaultFormats

  def this() = this(ConfigHelper.retrieve.getString("modelResolver.jslResolver.registryFile"))

  def getRegistry: Option[RegistryFile] = {
    try {
      val response: HttpResponse[String] = Http(registryRepo).asString
      parseResponse(response.body)
    } catch {
      case _: Throwable => Some(RegistryFile(List[RegistryModel]()))
    }
  }

  def parseResponse(response: String): Option[RegistryFile] = {
    val json = parse(response)
    val models = json.extract[JsonRegistry].modelTypes.flatMap { modelType: JsonModelType =>
      modelType.models.map { model =>
       RegistryModel(model.modelName, modelType.modelType, ModelVersion(model.modelVersion), model.modelUri)
      }
    }
    registryFile = Some(RegistryFile(models))
    registryFile
  }

  def retrieve(onlineModel: OnlineModel): Option[DownloadedModel] = {
    if (getRegistryFile.hasModel(onlineModel)) {
      val m: RegistryModel = getRegistryFile.findModel(onlineModel)

      try {
        val modelFileName = createModelFileName(onlineModel)
        val modelFullPath = Paths.get(getUserStore, modelFileName).toAbsolutePath.toString
        new URL(m.modelUri) #> new File(modelFullPath)
        Some(DownloadedModel(modelFullPath))
      } catch {
        case _: Throwable => None
      }
    } else {
      None
    }
  }
}
