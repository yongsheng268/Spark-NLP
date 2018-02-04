package com.johnsnowlabs.util

import org.scalatest.{FlatSpec, Matchers}
import scala.language.reflectiveCalls

class AnnotatorCorpusSpec extends FlatSpec with Matchers {
  "An AnnotatorCorpus" should "have a name and a version" in {
    val (name, ver) = ("somename", "1.1.0")
    val corpus = AnnotatorCorpus().setName(name).setVersion(ver)
    assert(corpus.name == name && corpus.version == ver)
  }

  it should "return the file path when the corpus has been downloaded" in {

  }
}
