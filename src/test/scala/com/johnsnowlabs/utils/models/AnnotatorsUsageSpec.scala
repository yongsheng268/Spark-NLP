package com.johnsnowlabs.utils.models

import com.johnsnowlabs.nlp.DataBuilder
import org.scalatest.{FlatSpec, Matchers}
import com.johnsnowlabs.nlp.annotators.pos.perceptron.{PerceptronApproach, PerceptronModel}

class AnnotatorsUsageSpec extends FlatSpec with Matchers {

  "A PerceptronModel" should "use the model resolver" in {
    val pos = new PerceptronApproach()
      .setModelResolver("default")
      .setRModelName("v1")
      .setRModelType("pos")
      .fit(DataBuilder.basicDataBuild("dummy"))

    assert(ModelDownloader.modelDownloaders("default").isCached, "Default resolver hasn't been used")
  }

  it should "cache the annotator" in {

  }
}
