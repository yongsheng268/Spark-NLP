package com.johnsnowlabs.util.resolvers

import com.johnsnowlabs.util.AnnotatorCorpus
import com.johnsnowlabs.util.resolvers.corpus.{CorpusResolver, HttpCorpusResolver}
import com.johnsnowlabs.util.resolvers.corpus.common.CorpusRegistry
import com.johnsnowlabs.util.resolvers.manager.BaseManager
import com.johnsnowlabs.util.store.ResourceStoreManager
import com.johnsnowlabs.util.store.common.StoredResource

import scala.collection.mutable

object CorpusManager extends BaseManager {
  type CorpusResolverName = String

  var resolverName: CorpusResolverName = "default"

  var resolvers: mutable.Map[CorpusResolverName, CorpusResolver] = mutable.Map[CorpusResolverName, CorpusResolver](
    "default" -> HttpCorpusResolver()
  )

  def getResolver: CorpusResolver = resolvers(resolverName)


  def retrieve(corpus: AnnotatorCorpus): DownloadedResource[AnnotatorCorpus] = {
    if (this.isCorpusCached(corpus)) {
      this.retrieveFromCache(corpus)
    } else {
      this.retrieveFromRepo(corpus)
    }
  }

  def isCorpusCached(corpus: AnnotatorCorpus): Boolean = {
    false
  }

  def retrieveFromCache(corpus: AnnotatorCorpus): DownloadedResource[AnnotatorCorpus] = {
    DownloadedResource[AnnotatorCorpus]("", corpus)
  }

  def retrieveFromRepo(corpus: AnnotatorCorpus): DownloadedResource[AnnotatorCorpus] = {
    val resolver = getResolver
    val corpusRegistry: CorpusRegistry = resolver.getRegistry.get

    if (corpusRegistry.hasCorpus(corpus)) {
      val body: Option[Array[Byte]] = resolver.getCorpus(corpusRegistry.findCorpus(corpus))
      val storedResource: StoredResource[AnnotatorCorpus] = ResourceStoreManager.createOrReplaceResource(corpus, body.get)
      DownloadedResource(storedResource.path, corpus)
    } else {
      throw new Exception("File not found")
    }
  }
}
