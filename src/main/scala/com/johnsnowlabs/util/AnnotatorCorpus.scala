package com.johnsnowlabs.util

import com.johnsnowlabs.util.resolvers.commons.SemVer

class AnnotatorCorpus(
                       var _name: String = "",
                       var _corpusType: String = "",
                       var _version: SemVer = SemVer("0.0.0")
                     ) extends AnnotatorResource[AnnotatorCorpus] {

  def setCorpusType(t: String): this.type = {
    _corpusType = t
    this
  }

  def corpusType: String = _corpusType

  def stringId: String = s"${name}-${version}-${corpusType}"
}

object AnnotatorCorpus {
  def apply(name: String, corpusType: String, ver: SemVer): AnnotatorCorpus =
    new AnnotatorCorpus(name, corpusType, ver)

  def apply(): AnnotatorCorpus = { new AnnotatorCorpus() }
}