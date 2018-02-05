package com.johnsnowlabs.util.resolvers.corpus

import com.johnsnowlabs.nlp.util.ConfigHelper
import com.johnsnowlabs.util.AnnotatorCorpus
import com.johnsnowlabs.util.resolvers.commons.SemVer
import com.johnsnowlabs.util.resolvers.corpus.common.CorpusRegistryResource
import com.johnsnowlabs.util.resolvers.corpus.common.CorpusRegistry
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scalaj.http.{Http, HttpResponse}


class HttpCorpusResolver(registryUri: String) extends CorpusResolver {
  implicit val formats: DefaultFormats = DefaultFormats

  case class JsonCorpus(modelName: String, modelVersion: String, modelUri: String)
  case class JsonCorpusType(corpusType: String, models: List[JsonCorpus])
  case class JsonCorpusRegistry(regVersion: String, corpusTypes: List[JsonCorpusType])

  def this() = this(ConfigHelper.retrieve.getString("corpusResolver.httpResolver.registryUri"))

  def getRegistry: Option[CorpusRegistry] = {
    try {
      val response: HttpResponse[String] = Http(registryUri).asString
      parseRegistry(response.body)
    } catch {
      case e: Throwable =>
        e.printStackTrace()
        None
    }
  }

  def parseRegistry(response: String): Option[CorpusRegistry] = {
    val json = parse(response)
    val models = json.extract[JsonCorpusRegistry].corpusTypes.flatMap { corpusType: JsonCorpusType =>
      corpusType.models.map { model =>
        CorpusRegistryResource(model.modelName, corpusType.corpusType, SemVer(model.modelVersion), model.modelUri)
      }
    }
    Some(CorpusRegistry(models))
  }


  def getCorpus(corpus: CorpusRegistryResource): Option[Array[Byte]] = {
    val response: HttpResponse[Array[Byte]] = Http(corpus.corpusUri).asBytes
    val body: Array[Byte] = response.body
    Some(body)
  }

  def getCorpus(corpus: AnnotatorCorpus): Option[Array[Byte]] = {
    getCorpus(CorpusRegistryResource(corpus))
  }
}

object HttpCorpusResolver {
  def apply(): HttpCorpusResolver = new HttpCorpusResolver()
}