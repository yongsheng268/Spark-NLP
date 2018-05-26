package com.johnsnowlabs.nlp.annotators.pos.perceptron

import com.johnsnowlabs.nlp.annotators.common.{IndexedTaggedWord, TaggedSentence}
import com.johnsnowlabs.nlp.annotators.param.ExternalResourceParam
import com.johnsnowlabs.nlp.util.io.{ExternalResource, ReadAs, ResourceHelper}
import com.johnsnowlabs.nlp.{Annotation, AnnotatorApproach, AnnotatorType}
import com.johnsnowlabs.util.Benchmark
import com.johnsnowlabs.util.spark.{DoubleMapAccumulatorWithDefault, LongMapAccumulatorWithDefault, TupleKeyLongMapAccumulatorWithDefault}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.param.{IntParam, Param}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.util.{AccumulatorV2, CollectionAccumulator, LongAccumulator}

import scala.collection.mutable.{Map => MMap}
import scala.util.Random

/**
  * Created by Saif Addin on 5/17/2017.
  * Inspired on Averaged Perceptron by Matthew Honnibal
  * https://explosion.ai/blog/part-of-speech-pos-tagger-in-python
  */
class PerceptronApproach(override val uid: String) extends AnnotatorApproach[PerceptronModel] with PerceptronUtils {

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

  val featuresWeight = new StringMapStringDoubleAccumulator()
  val timestamps = new TupleKeyLongMapAccumulatorWithDefault()
  val updateIteration = new LongAccumulator()
  val totals = new StringTupleDoubleAccumulatorWithDV()
  ResourceHelper.spark.sparkContext.register(featuresWeight)
  ResourceHelper.spark.sparkContext.register(timestamps)
  ResourceHelper.spark.sparkContext.register(updateIteration)
  ResourceHelper.spark.sparkContext.register(totals)

  /**
    * This is model learning tweaking during training, in-place
    * Uses mutable collections since this runs per word, not per iteration
    * Hence, performance is needed, without risk as long as this is a
    * non parallel training running outside spark
    * @return
    */
  @volatile
  def update(
              truth: String,
              guess: String,
              features: Map[String, Int],
              ii: Long,
              bb: Map[String, Map[String, Double]],
              tt: Map[(String, String), Long]) = {
    val b = MMap.empty[String, Map[String, Double]]
    val t = MMap.empty[(String, String), Long].withDefaultValue(0L)
    def updateFeature(tag: String, feature: String, weight: Double, value: Double): Unit = {
      val param = (feature, tag)
      /**
        * update totals and timestamps
        */
      totals.add((param, (ii - t(param)) * weight))
      //totals.add(param, (updateIteration.value - timestamps.value(param)) * weight)
      //timestamps(param) = updateIteration.value
      t.update(param, ii)
      /**
        * update weights
        */
      b.update(feature, b.getOrElseUpdate(feature, Map()) ++ MMap(tag -> (weight + value)))
      //featuresWeight.add(feature, Map(tag -> (weight + value)))
      //featuresWeight.innerSet((feature, tag), weight + value)
      //featuresWeight(feature)(tag) = weight + value
      //featuresWeight.value(feature) = MMap(tag -> (weight + value))
    }
    /**
      * if prediction was wrong, take all features and for each feature get feature's current tags and their weights
      * congratulate if success and punish for wrong in weight
      */
    if (truth != guess) {
      features.foreach{case (feature, _) =>
        val weights = b.getOrElseUpdate(feature, Map())
        updateFeature(truth, feature, weights.getOrElse(truth, 0.0), 1.0)
        updateFeature(guess, feature, weights.getOrElse(guess, 0.0), -1.0)
      }
    }
    updateIteration.add(1)
    timestamps.updateMany(t)
    featuresWeight.addMany(b)
    (t, b)
  }

  private[pos] def averageWeights(tags: Array[String], taggedWordBook: Broadcast[Map[String, String]]): AveragedPerceptron = {
    val fw = featuresWeight.value
    val uiv = updateIteration.value
    val tv = totals.value
    val tmv = timestamps.value
    featuresWeight.reset()
    updateIteration.reset()
    totals.reset()
    timestamps.reset()
    val finalfw = Benchmark.time("Average weighting took") {fw.map { case (feature, weights) =>
      (feature, weights.map { case (tag, weight) =>
        val param = (feature, tag)
        val total = tv(param) + ((uiv - tmv(param)) * weight)
        (tag, total / uiv.toDouble)
      })
    }}
    val apr = new AveragedPerceptron(
      tags,
      taggedWordBook.value,
      finalfw
    )
    taggedWordBook.destroy()
    apr
  }

  /**
    * Trains a model based on a provided CORPUS
    *
    * @return A trained averaged model
    */
  @volatile
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
      ResourceHelper.parseTupleSentencesDS($(corpus)).repartition(16).cache
    }
    val taggedWordBook = dataset.sparkSession.sparkContext.broadcast(buildTagBook(taggedSentences))
    /** finds all distinct tags and stores them */
    val classes = taggedSentences.flatMap(_.tags).distinct.collect

    /**
      * Iterates for training
      */
    Benchmark.time("Time for iterations") { (1 to $(nIterations)).foreach { iteration => {
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
        //val bb = MMap(featuresWeight.value.toSeq:_*).withDefaultValue(Map.empty[String, Double].withDefaultValue(0.0))
        var bb = featuresWeight.value
        //val tt = MMap(timestamps.value.toSeq:_*).withDefaultValue(0L)
        var tt = timestamps.value
        var ii = updateIteration.value
        taggedSentence.words.zipWithIndex.foreach { case (word, i) =>
          val guess = taggedWordBook.value.getOrElse(word.toLowerCase,{
            /**
              * if word is not found, collect its features which are used for prediction and predict
              */
            val features = getFeatures(i, word, context, prev, prev2)
            val model = new AveragedPerceptron(classes, taggedWordBook.value, bb.withDefaultValue(Map.empty[String, Double].withDefaultValue(0.0)))
            val guess = model.predict(features)
            /**
              * Update the model based on the prediction results
              */
            val (t, b) = update(taggedSentence.tags(i), guess, features.toMap, ii, bb, tt)
            tt ++= t
            bb ++= b
            ii += 1
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
        }
      }
        //iteratedModel.unpersist(true)
      }}}}
    //trainedModel.value.averageWeights()
    //trainedModel.unpersist(true)
    //println(s"WEIGHT SIZE: ${featuresWeight.value.size} INNER SIZE: ${featuresWeight.value.size}")
    //println(s"TIMESTAMP SIZE: ${timestamps.value.size}")
    println(s"ITERATION: ${updateIteration.value}")
    logger.debug("TRAINING: Finished all iterations")
    new PerceptronModel().setModel(averageWeights(classes, taggedWordBook))
  }
}