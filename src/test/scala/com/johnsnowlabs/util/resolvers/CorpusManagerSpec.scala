package com.johnsnowlabs.util.resolvers

import com.johnsnowlabs.util.AnnotatorCorpus
import org.scalatest.{FlatSpec, Matchers}

import scala.language.reflectiveCalls

class CorpusManagerSpec extends FlatSpec with Matchers {
  def fixture() = new {
    val corpus = AnnotatorCorpus().setName("anc-pos-corpus").setCorpusType("pos")
  }

  "A CorpusManager" should "download a Corpus" in {
    val f = fixture()
    CorpusManager.retrieve(f.corpus)
  }
}
