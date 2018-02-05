package com.johnsnowlabs.util.resolvers

import com.johnsnowlabs.util.AnnotatorCorpus
import org.scalatest.{FlatSpec, Matchers}

import scala.language.reflectiveCalls
import scala.reflect.io.File

class CorpusManagerSpec extends FlatSpec with Matchers {
  def fixture() = new {
    val corpus = AnnotatorCorpus().setName("anc-pos-corpus").setCorpusType("pos").setVersion("0.0.1")
  }

  "A CorpusManager" should "download a Corpus" in {
    val f = fixture()
    val c: DownloadedResource[AnnotatorCorpus] = CorpusManager.retrieve(f.corpus)
    c.path should not be empty
    File(c.path).exists shouldBe true
  }
}
