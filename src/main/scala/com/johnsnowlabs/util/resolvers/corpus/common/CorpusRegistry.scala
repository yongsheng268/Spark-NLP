package com.johnsnowlabs.util.resolvers.corpus.common

import com.johnsnowlabs.util.AnnotatorCorpus

case class CorpusRegistry(
                      modelList: Seq[CorpusRegistryResource]
                    ) {
  def length: Int = this.modelList.length

  def size = length

  def hasCorpus(corpus: CorpusRegistryResource): Boolean = {
    modelList.exists {
      case CorpusRegistryResource(corpusName, corpusType, corpusVersion, _) =>
        corpus.corpusName == corpusName && corpus.corpusType == corpusType && corpus.corpusVersion == corpusVersion
    }
  }

  def hasCorpus(corpus: AnnotatorCorpus): Boolean = {
    hasCorpus(CorpusRegistryResource(corpus))
  }

  def findCorpus(corpus: CorpusRegistryResource): CorpusRegistryResource = {
    modelList.find {
      case CorpusRegistryResource(corpusName, corpusType, corpusVersion, _) =>
        corpus.corpusName == corpusName && corpus.corpusType == corpusType && corpus.corpusVersion == corpusVersion
    }.get
  }
}
