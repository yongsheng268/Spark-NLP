package com.johnsnowlabs.util.resolvers.corpus

import java.io.InputStream

import com.johnsnowlabs.util.AnnotatorCorpus
import com.johnsnowlabs.util.resolvers.corpus.common.{CorpusRegistry, CorpusRegistryResource}

trait CorpusResolver {
  def getRegistry: Option[CorpusRegistry]
  def getCorpus(corpus: CorpusRegistryResource): Option[Array[Byte]]
  def getCorpus(corpus: AnnotatorCorpus): Option[Array[Byte]]
}
