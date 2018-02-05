package com.johnsnowlabs.util.resolvers.model

import com.johnsnowlabs.nlp.util.ConfigHelper
import com.johnsnowlabs.util.AnnotatorOnlineModel
import com.johnsnowlabs.util.resolvers.commons.SemVer
import com.johnsnowlabs.util.resolvers.model.common.{ModelRegistry, ModelRegistryResource}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scalaj.http.{Http, HttpResponse}

class HttpModelResolver(registryUri: String) extends ModelResolver {
  implicit val formats: DefaultFormats = DefaultFormats

  case class JsonModel(modelName: String, modelVersion: String, modelUri: String, sparkVersion: String, sparkNlpVersion: String, lang: String)
  case class JsonModelType(modelType: String, models: List[JsonModel])
  case class JsonModelRegistry(regVersion: String, modelTypes: List[JsonModelType])

  def this() = this(ConfigHelper.retrieve.getString("modelResolver.httpResolver.registryUri"))

  def getRegistry: Option[ModelRegistry] = {
    try {
      val response: HttpResponse[String] = Http(registryUri).asString
      parseRegistry(response.body)
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        None
    }
  }

  def parseRegistry(response: String): Option[ModelRegistry] = {
    val json = parse(response)
    val models = json.extract[JsonModelRegistry].modelTypes.flatMap { modelType: JsonModelType =>
      modelType.models.map { model =>
        ModelRegistryResource(
          model.modelName,
          modelType.modelType,
          SemVer(model.modelVersion),
          model.modelUri,
          SemVer(model.sparkVersion),
          SemVer(model.sparkNlpVersion),
          model.lang
        )
      }
    }
    Some(ModelRegistry(models))
  }


  def getModel(model: ModelRegistryResource): Option[Array[Byte]] = {
    val response: HttpResponse[Array[Byte]] = Http(model.modelUri).asBytes
    val body: Array[Byte] = response.body
    Some(body)
  }

  def getModel(model: AnnotatorOnlineModel): Option[Array[Byte]] = {
    getModel(ModelRegistryResource(model))
  }

}

object HttpModelResolver {
  def apply(): HttpModelResolver = new HttpModelResolver()
}
