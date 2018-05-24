package com.johnsnowlabs.nlp.annotators.pos.perceptron

import com.johnsnowlabs.util.spark.{DoubleMapAccumulatorWithDefault, TupleKeyLongMapAccumulatorWithDefault}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.LongAccumulator

import scala.collection.mutable.{ArrayBuffer, Map => MMap}

/**
  * Created by Saif Addin on 5/16/2017.
  */

/**
  * Specific model for [[PerceptronApproach]]
  * @param tags Holds all unique tags based on training
  * @param taggedWordBook Contains non ambiguous words and their tags
  * @param featuresWeight Contains prediction information based on context frequencies
  * @param timestamps Contains timestamp broadcast
  * @param updateIteration Contains information on how many iterations have run for weighting
  */
class AveragedPerceptron(
                          spark: SparkSession,
                          tags: Array[String],
                          taggedWordBook: Broadcast[Map[String, String]],
                          featuresWeight: StringMapStringDoubleAccumulatorWithDVMutable,
                          timestamps: TupleKeyLongMapAccumulatorWithDefault,
                          updateIteration: LongAccumulator
                         ) extends Serializable {

  /**How many training iterations ran*/
  /**totals contains scores for words and their possible tags*/
  private val totals = new StringTupleDoubleAccumulatorWithDV()
  spark.sparkContext.register(totals)

  def predict(features: List[(String, Int)]): String = {
    /**
      * scores are used for feature scores, which are all by default 0
      * if a feature has a relevant score, look for all its possible tags and their scores
      * multiply their weights per the times they appear
      * Return highest tag by score
      *
      */
    val scoresByTag = features
      .filter{case (feature, value) => featuresWeight.value.contains(feature) && value != 0}
      .map{case (feature, value ) =>
        featuresWeight.value(feature)
          .map{ case (tag, weight) =>
            (tag, value * weight)
          }
      }.aggregate(MMap[String, Double]())(
      (tagsScores, tagScore) => tagScore ++ tagsScores.map{case(tag, score) => (tag, tagScore.getOrElse(tag, 0.0) + score)},
      (pTagScore, cTagScore) => pTagScore.map{case (tag, score) => (tag, cTagScore.getOrElse(tag, 0.0) + score)}
    )
    /**
      * ToDo: Watch it here. Because of missing training corpus, default values are made to make tests pass
      * Secondary sort by tag simply made to match original python behavior
      */
    tags.maxBy{ tag => (scoresByTag.withDefaultValue(0.0)(tag), tag)}
  }

  /**
    * Training level operation
    * once a model was trained, average its weights more in the first iterations
    */
  private[pos] def averageWeights(): Unit = {
    featuresWeight.value.foreach { case (feature, weights) =>
      featuresWeight.update(feature,
        weights.map { case (tag, weight) =>
          val param = (feature, tag)
          val total = totals.value(param) + ((updateIteration.value - timestamps.value(param)) * weight)
          (tag, total / updateIteration.value.toDouble)
        }
      )
    }
  }
  private[nlp] def getUpdateIterations: Long = updateIteration.value
  private[nlp] def getTagBook: Map[String, String] = taggedWordBook.value
  private[nlp] def getTags: Array[String] = tags
  def getWeights: Map[String, Map[String, Double]] = featuresWeight.value.mapValues(_.toMap).toMap
  /**
    * This is model learning tweaking during training, in-place
    * Uses mutable collections since this runs per word, not per iteration
    * Hence, performance is needed, without risk as long as this is a
    * non parallel training running outside spark
    * @return
    */
  def update(truth: String, guess: String, features: Map[String, Int]): Unit = {
    val a = MMap(timestamps.value.toSeq:_*)
    def updateFeature(tag: String, feature: String, weight: Double, value: Double) = {
      val param = (feature, tag)
      /**
        * update totals and timestamps
        */
      totals.add(param, (updateIteration.value - a.getOrElse(param, timestamps.value(param))) * weight)
      a(param) = updateIteration.value
      /**
        * update weights
        */
      featuresWeight.innerSet((feature, tag), weight + value)
    }
    updateIteration.add(1)
    /**
      * if prediction was wrong, take all features and for each feature get feature's current tags and their weights
      * congratulate if success and punish for wrong in weight
      */
    if (truth != guess) {
      features.foreach{case (feature, _) =>
        val weights = featuresWeight.value(feature)
        updateFeature(truth, feature, weights.getOrElse(truth, 0.0), 1.0)
        updateFeature(guess, feature, weights.getOrElse(guess, 0.0), -1.0)
      }
    }
    timestamps.updateMany(a)
  }
}