package com.johnsnowlabs.util.resolvers.corpus.common

import com.johnsnowlabs.util.AnnotatorCorpus
import com.johnsnowlabs.util.resolvers.commons.SemVer

case class CorpusRegistryResource(
                                   corpusName: String,
                                   corpusType: String,
                                   corpusVersion: SemVer,
                                   corpusUri: String
                         ) {

}

object CorpusRegistryResource {
  def apply(corpus: AnnotatorCorpus) = new CorpusRegistryResource(
    corpus.name,
    corpus.corpusType,
    corpus.version,
    ""
  )
}
