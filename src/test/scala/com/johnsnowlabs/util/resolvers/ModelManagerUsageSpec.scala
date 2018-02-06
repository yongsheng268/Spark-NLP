package com.johnsnowlabs.util.resolvers

import com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronModel
import com.johnsnowlabs.util.AnnotatorOnlineModel
import com.johnsnowlabs.util.resolvers.commons.SemVer
import org.scalatest.{BeforeAndAfterEach, FlatSpec, Matchers}
import com.johnsnowlabs.nlp._
import org.apache.spark.ml.PipelineModel
import com.johnsnowlabs.nlp.implicits._

import scala.language.reflectiveCalls

class ModelManagerUsageSpec extends FlatSpec with Matchers with BeforeAndAfterEach {

  SparkAccessor.spark
  import SparkAccessor.spark.implicits._

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
    val posModel: PerceptronModel = PerceptronModel.load(ModelManager.retrieve("pos-test", "pos", "0.0.1"))
    val posDF = posModel
      .setOutputCol("pos")
      .setInputCols(Array("sentence", "token")).transform(f.df)

    val annotations = Annotation.collect(posDF, "pos")

    annotations.length should be > 0
    annotations.flatMap { _.toSeq }.foreach( (a: Annotation) =>
      a.annotatorType shouldBe "pos"
    )
  }

  it should "return a pipeline sentiment" in {
    val f = fixture()
    val onlineModel: AnnotatorOnlineModel = AnnotatorOnlineModel("pipeline-sentiment", "pipeline-test", SemVer("0.0.1"))
    val pipeline: PipelineModel = PipelineModel.load(ModelManager.retrieve(onlineModel))
    val df = pipeline.transform(f.df.withColumn("review", $"text"))
    val annotations = Annotation.collect(df, "spell")
    annotations.length should be > 0
    annotations.flatMap { _.toSeq }.foreach { (a: Annotation) =>
      a.annotatorType shouldBe "token"
    }
  }
}
