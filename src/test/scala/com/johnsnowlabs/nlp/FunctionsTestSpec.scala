package com.johnsnowlabs.nlp

import com.johnsnowlabs.nlp.annotator.Tokenizer
import com.johnsnowlabs.nlp.annotators.pos.perceptron.PerceptronModel
import org.apache.spark.ml.Pipeline
import org.scalatest._

class FunctionsTestSpec extends FlatSpec {

  "functions in functions" should "work successfully" in {

    import com.johnsnowlabs.nlp.util.io.ResourceHelper.spark.implicits._

    val documentAssembler = new DocumentAssembler()
      .setInputCol("text")
      .setOutputCol("document")

    val tokenizer = new Tokenizer()
      .setInputCols(Array("document"))
      .setOutputCol("token")

    val pos = PerceptronModel.pretrained()
      .setInputCols("document", "token")
      .setOutputCol("pos")

    val pipeline = new Pipeline()
      .setStages(Array(
        documentAssembler,
        tokenizer,
        pos
      ))

    val model = pipeline.fit(Seq.empty[String].toDF("text"))
    val singleLineData = model.transform(Seq("Peter is a very good and compromised person.").toDF("text"))

    import functions._

    val mapped = singleLineData.mapAnnotations("pos", "modpos", (annotations: Seq[Annotation]) => {
      annotations.filter(_.result == "JJ")
    })

    val modified = singleLineData.mapAnnotations("pos", "modpos", (_: Seq[Annotation]) => {
      "hello world"
    })

    val filtered = singleLineData.filterByAnnotations("pos", (annotations: Seq[Annotation]) => {
      annotations.exists(_.result == "JJ")
    })

    val grouped = singleLineData.groupAnnotations("pos", "grouped", { annotation =>
      annotation.result.contains("B")
    })



    mapped.show(truncate = false)
    modified.show(truncate = false)
    filtered.show(truncate = false)
  }

}
