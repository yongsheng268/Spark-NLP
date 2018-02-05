package com.johnsnowlabs.util.resolvers

import com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronModel
import com.johnsnowlabs.util.AnnotatorOnlineModel
import com.johnsnowlabs.util.resolvers.commons.SemVer
import org.scalatest.{FlatSpec, Matchers}
import com.johnsnowlabs.nlp._
import org.apache.spark.sql.Row

import scala.language.reflectiveCalls

class ModelManagerUsageSpec extends FlatSpec with Matchers {
  SparkAccessor.spark

  def fixture() = new {
    val df = AnnotatorBuilder.withTokenizer(
      AnnotatorBuilder.withFullPragmaticSentenceDetector(
        AnnotatorBuilder.withDocumentAssembler(
          DataBuilder.basicDataBuild(
            ContentProvider.englishPhrase
          )
        )
      )
    )
  }

  "A ModelManager" should "return an stored pos model ready to be loaded" in {
    val f = fixture()
    val onlineModel: AnnotatorOnlineModel = AnnotatorOnlineModel("pos-test", "pos", SemVer("0.0.1"))
    val posModel: PerceptronModel = PerceptronModel.load(ModelManager.retrieve(onlineModel).path)
    val posDF = posModel
      .setOutputCol("pos")
      .setInputCols(Array("sentence", "token")).transform(f.df)

    val annotations = Annotation.collect(posDF, "pos")
    annotations.length should be > 0
    annotations.flatMap { _.toSeq }.foreach( (a: Annotation) =>
      a.annotatorType shouldBe "pos"
    )
  }
}
