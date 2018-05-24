package com.johnsnowlabs.nlp.annotators.pos.perceptron

import com.johnsnowlabs.nlp.annotators.common.{IndexedTaggedWord, TaggedSentence}
import com.johnsnowlabs.nlp.annotators.param.ExternalResourceParam
import com.johnsnowlabs.nlp.util.io.{ExternalResource, ReadAs, ResourceHelper}
import com.johnsnowlabs.nlp.{Annotation, AnnotatorApproach, AnnotatorType}
import com.johnsnowlabs.util.Benchmark
import com.johnsnowlabs.util.spark.{DoubleMapAccumulatorWithDefault, LongMapAccumulatorWithDefault, TupleKeyDoubleMapAccumulatorWithDefault}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.param.{IntParam, Param}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.{AccumulatorV2, CollectionAccumulator}

import scala.collection.mutable.{Map => MMap}
import scala.util.Random

/**
  * Created by Saif Addin on 5/17/2017.
  * Inspired on Averaged Perceptron by Matthew Honnibal
  * https://explosion.ai/blog/part-of-speech-pos-tagger-in-python
  */
class PerceptronApproachDS(override val uid: String) extends AnnotatorApproach[PerceptronModel] with PerceptronUtils {

  import com.johnsnowlabs.nlp.AnnotatorType._

  override val description: String = "Averaged Perceptron model to tag words part-of-speech"

  val posCol = new Param[String](this, "posCol", "column of Array of POS tags that match tokens")
  val corpus = new ExternalResourceParam(this, "corpus", "POS tags delimited corpus. Needs 'delimiter' in options")
  val nIterations = new IntParam(this, "nIterations", "Number of iterations in training, converges to better accuracy")

  setDefault(nIterations, 5)

  def setPosColumn(value: String): this.type = set(posCol, value)

  def setCorpus(value: ExternalResource): this.type = {
    require(value.options.contains("delimiter"), "PerceptronApproach needs 'delimiter' in options to associate words with tags")
    set(corpus, value)
  }

  def setCorpus(path: String,
                delimiter: String,
                readAs: ReadAs.Format = ReadAs.LINE_BY_LINE,
                options: Map[String, String] = Map("format" -> "text")): this.type =
    set(corpus, ExternalResource(path, readAs, options ++ Map("delimiter" -> delimiter)))

  def setNIterations(value: Int): this.type = set(nIterations, value)

  def this() = this(Identifiable.randomUID("POS"))

  override val annotatorType: AnnotatorType = POS

  override val requiredAnnotatorTypes: Array[AnnotatorType] = Array(TOKEN, DOCUMENT)

  /**
    * Finds very frequent tags on a word in training, and marks them as non ambiguous based on tune parameters
    * ToDo: Move such parameters to configuration
    *
    * @param taggedSentences    Takes entire tagged sentences to find frequent tags
    * @param frequencyThreshold How many times at least a tag on a word to be marked as frequent
    * @param ambiguityThreshold How much percentage of total amount of words are covered to be marked as frequent
    */
  private def buildTagBook(
                            taggedSentences: Dataset[TaggedSentence],
                            frequencyThreshold: Int = 20,
                            ambiguityThreshold: Double = 0.97
                          ): Map[String, String] = {
    import ResourceHelper.spark.implicits._
    val tagFrequenciesByWord = taggedSentences
      .flatMap(_.taggedWords)
      .groupByKey(tw => tw.word.toLowerCase)
      .mapGroups{case (lw, tw) => (lw, tw.toSeq.groupBy(_.tag).mapValues(_.length))}
      .filter { lwtw =>
        val (_, mode) = lwtw._2.maxBy(t => t._2)
        val n = lwtw._2.values.sum
        n >= frequencyThreshold && (mode / n.toDouble) >= ambiguityThreshold
      }

    tagFrequenciesByWord.map { case (word, tagFrequencies) =>
      val (tag, _) = tagFrequencies.maxBy(_._2)
      logger.debug(s"TRAINING: Ambiguity discarded on: << $word >> set to: << $tag >>")
      (word, tag)
    }.collect.toMap
  }

  /**
    * Trains a model based on a provided CORPUS
    *
    * @return A trained averaged model
    */
  override def train(dataset: Dataset[_], recursivePipeline: Option[PipelineModel]): PerceptronModel = {
    /**
      * Generates TagBook, which holds all the word to tags mapping that are not ambiguous
      */
    import ResourceHelper.spark.implicits._
    val taggedSentences: Dataset[TaggedSentence] = if (get(posCol).isDefined) {
      val tokenColumn = dataset.schema.fields
        .find(f => f.metadata.contains("annotatorType") && f.metadata.getString("annotatorType") == AnnotatorType.TOKEN)
        .map(_.name).get
      dataset.select(tokenColumn, $(posCol))
        .as[(Array[Annotation], Array[String])]
        .map{
          case (annotations, posTags) =>
            lazy val strTokens = annotations.map(_.result).mkString("#")
            lazy val strPosTags = posTags.mkString("#")
            require(annotations.length == posTags.length, s"Cannot train from $posCol since there" +
              s" is a row with different amount of tags and tokens:\n$strTokens\n$strPosTags")
            TaggedSentence(annotations.zip(posTags)
              .map{case (annotation, posTag) => IndexedTaggedWord(annotation.result, posTag, annotation.begin, annotation.end)}
            )
        }
    } else {
      ResourceHelper.parseTupleSentencesDS($(corpus))
    }
    val taggedWordBook = dataset.sparkSession.sparkContext.broadcast(buildTagBook(taggedSentences))
    /** finds all distinct tags and stores them */
    val classes = taggedSentences.flatMap(_.tags).distinct.collect
    val weightCollection = new StringMapStringDoubleAccumulatorWithDVMutable()
    val timestampsCollection = new TupleKeyDoubleMapAccumulatorWithDefault()
    dataset.sparkSession.sparkContext.register(weightCollection)
    dataset.sparkSession.sparkContext.register(timestampsCollection)
    val initialModel = new AveragedPerceptron(
      dataset.sparkSession,
      classes,
      taggedWordBook,
      weightCollection,
      timestampsCollection
    )
    /**
      * Iterates for training
      */
    val trainedModel = Benchmark.time("Time for iterations") { (1 to $(nIterations)).foldLeft(initialModel) { (iteratedModel, iteration) => {
      logger.debug(s"TRAINING: Iteration n: $iteration")
      /**
        * In a shuffled sentences list, try to find tag of the word, hold the correct answer
        */
      Benchmark.time("Time for iteration") { taggedSentences.foreach{taggedSentence =>

        /**
          * Defines a sentence context, with room to for look back
          */
        var prev = START(0)
        var prev2 = START(1)
        val context = START ++: taggedSentence.words.map(w => normalized(w)) ++: END
        Benchmark.time("Time for sentence words") {taggedSentence.words.zipWithIndex.foreach { case (word, i) =>
            val guess = taggedWordBook.value.getOrElse(word.toLowerCase,{
              /**
                * if word is not found, collect its features which are used for prediction and predict
                */
              val features = getFeatures(i, word, context, prev, prev2)
              val guess = iteratedModel.predict(features)
              /**
                * Update the model based on the prediction results
                */
              iteratedModel.update(taggedSentence.tags(i), guess, features.toMap)
              /**
                * return the guess
                */
              guess
            })
            /**
              * shift the context
              */
            prev2 = prev
            prev = guess
        }}
      }
      iteratedModel
    }}}}
    trainedModel.averageWeights()
    logger.debug("TRAINING: Finished all iterations")
    new PerceptronModel().setModel(trainedModel)
  }
}

