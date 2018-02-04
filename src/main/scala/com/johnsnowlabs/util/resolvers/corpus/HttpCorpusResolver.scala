package com.johnsnowlabs.util.resolvers.corpus

import java.io.{FileOutputStream, InputStream}
import java.net.URL

import com.johnsnowlabs.nlp.util.ConfigHelper
import com.johnsnowlabs.util.AnnotatorCorpus
import com.johnsnowlabs.util.resolvers.commons.SemVer
import com.johnsnowlabs.util.resolvers.corpus.common.CorpusRegistryResource
import com.johnsnowlabs.util.resolvers.corpus.common.CorpusRegistry
import com.johnsnowlabs.utils.models._
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods.parse

import scalaj.http.{Http, HttpResponse}

class HttpCorpusResolver(registryUri: String) extends CorpusResolver {
  implicit val formats = DefaultFormats

  def this() = this(ConfigHelper.retrieve.getString("corpusResolver.httpResolver.registryUri"))

  def getRegistry: Option[CorpusRegistry] = {
    try {
      val response: HttpResponse[String] = Http(registryUri).asString
      parseRegistry(response.body)
    } catch {
      case _: Throwable => Some(CorpusRegistry(List[CorpusRegistryResource]()))
    }
  }

  def parseRegistry(response: String): Option[CorpusRegistry] = {
    val json = parse(response)
    val models = json.extract[JsonRegistry].modelTypes.flatMap { modelType: JsonModelType =>
      modelType.models.map { model =>
        CorpusRegistryResource(model.modelName, modelType.modelType, SemVer(model.modelVersion), model.modelUri)
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
  def apply() = new HttpCorpusResolver()
}