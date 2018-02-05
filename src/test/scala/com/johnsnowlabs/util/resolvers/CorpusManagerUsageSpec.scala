package com.johnsnowlabs.util.resolvers

import com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronApproach
import com.johnsnowlabs.nlp.{ContentProvider, DocumentAssembler}
import com.johnsnowlabs.nlp.annotators.Tokenizer
import com.johnsnowlabs.nlp.annotators.sbd.pragmatic.SentenceDetector
import com.johnsnowlabs.util.AnnotatorCorpus
import org.apache.spark.ml.Pipeline
import org.scalatest.{FlatSpec, Matchers}

import scala.language.reflectiveCalls

class CorpusManagerUsageSpec extends FlatSpec with Matchers{
  def fixture() = new {
    val data = ContentProvider.parquetData.limit(1000)

    val documentAssembler = new DocumentAssembler()
      .setInputCol("text")
      .setOutputCol("document")

    val sentence = new SentenceDetector()
      .setInputCols("document")
      .setOutputCol("sentence")

    val tokenizer = new Tokenizer()
      .setInputCols(Array("document"))
      .setOutputCol("token")

    val pos = new PerceptronApproach()
      .setInputCols(Array("token", "sentence"))
      .setOutputCol("pos")
      .setNIterations(3)
      .setCorpusLimit(5)
  }

  "A PerceptronApproach" can "use a DownloadedResource" in {
    val f = fixture()
    val corpus = AnnotatorCorpus().setName("anc-pos-corpus").setVersion("0.0.1").setCorpusType("pos")
    val resource = CorpusManager.retrieve(corpus)
    f.pos.setCorpusPath(resource.path)
    val pipeline = new Pipeline().setStages(Array(f.documentAssembler, f.sentence, f.tokenizer, f.pos))
    pipeline.fit(f.data)
  }
}
