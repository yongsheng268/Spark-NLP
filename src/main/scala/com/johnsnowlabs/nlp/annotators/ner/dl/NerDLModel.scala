package com.johnsnowlabs.nlp.annotators.ner.dl


import java.io.File
import java.nio.file.Paths

import com.johnsnowlabs.ml.tensorflow.TensorflowWrapper.readGraph
import com.johnsnowlabs.ml.tensorflow._
import com.johnsnowlabs.nlp.AnnotatorType._
import com.johnsnowlabs.nlp._
import com.johnsnowlabs.nlp.annotators.common.Annotated.NerTaggedSentence
import com.johnsnowlabs.nlp.annotators.common._
import com.johnsnowlabs.nlp.annotators.ner.Verbose
import com.johnsnowlabs.nlp.pretrained.ResourceDownloader
import com.johnsnowlabs.nlp.serialization.StructFeature
import org.apache.spark.SparkFiles
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.ml.param.{FloatParam, IntParam}
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{Dataset, SparkSession}
import org.tensorflow.Session


class NerDLModel(override val uid: String)
  extends AnnotatorModel[NerDLModel]
    with WriteTensorflowModel
    with ParamsAndFeaturesWritable
    with ReadsNERGraph
    with LoadsContrib {

  def this() = this(Identifiable.randomUID("NerDLModel"))

  override val inputAnnotatorTypes = Array(DOCUMENT, TOKEN, WORD_EMBEDDINGS)
  override val outputAnnotatorType = NAMED_ENTITY

  val minProba = new FloatParam(this, "minProbe", "Minimum probability. Used only if there is no CRF on top of LSTM layer.")
  def setMinProbability(minProba: Float) = set(this.minProba, minProba)

  val batchSize = new IntParam(this, "batchSize", "Size of every batch.")
  def setBatchSize(size: Int) = set(this.batchSize, size)

  val datasetParams = new StructFeature[DatasetEncoderParams](this, "datasetParams")
  def setDatasetParams(params: DatasetEncoderParams) = set(this.datasetParams, params)

  def tag(tokenized: Array[WordpieceEmbeddingsSentence]): Array[NerTaggedSentence] = {
    // Predict
    println("PREDICTING")
    val labels = getModelIfNotSet.predict(tokenized)

    // Combine labels with sentences tokens
    tokenized.indices.map { i =>
      val sentence = tokenized(i)

      val tokens = sentence.tokens.indices.flatMap { j =>
        val token = sentence.tokens(j)
        val label = labels(i)(j)
        if (token.isWordStart) {
          Some(IndexedTaggedWord(token.token, label, token.begin, token.end))
        }
        else {
          None
        }
      }.toArray

      new TaggedSentence(tokens)
    }.toArray
  }

  def getTensorflowIfNotSet: TensorflowWrapper = {
    if (tensorflow == null) {
      println("TENSORFLOW IS NULL IN GET TENSORFLOW")
      val target = Paths.get(SparkFiles.getRootDirectory(), "tensorflow").toString
      val path = if (new File(target).exists()) target else SparkFiles.get("tensorflow")
      setTensorflow(TensorflowWrapper.read(path))
    }
    tensorflow
  }

  @transient var tensorflow: TensorflowWrapper = null

  def setTensorflow(tf: TensorflowWrapper): NerDLModel = {
    this.tensorflow = tf
    this
  }

  def getModelIfNotSet = {
    if (_model == null) {
      require(datasetParams.isSet, "datasetParams must be set before usage")
      if (tensorflow == null) {
        println("TENSORFLOW IS NULL IN GET MODEL")
        println("Local tensorflow not set. Re setting")
        getTensorflowIfNotSet
      }

      val encoder = new NerDatasetEncoder(datasetParams.get.get)
      _model =
        new TensorflowNer(
          tensorflow,
          encoder,
          1, // Tensorflow doesn't clear state in batch
          Verbose.Silent
        )
    }
    _model
  }

  @transient private var _model: TensorflowNer = null

  override def beforeAnnotate(dataset: Dataset[_]): Dataset[_] = {

    dataset.foreachPartition(_ => {
      getModelIfNotSet
    })

    loadContribToCluster(dataset.sparkSession)

    dataset
  }

  override def annotate(annotations: Seq[Annotation]): Seq[Annotation] = {

    // Parse
    val tokenized = WordpieceEmbeddingsSentence.unpack(annotations).toArray

    // Predict
    val tagged = tag(tokenized)

    // Pack
    NerTagged.pack(tagged)
  }


  override def onWrite(path: String, spark: SparkSession): Unit = {
    super.onWrite(path, spark)
    writeTensorflowModel(path, spark, getModelIfNotSet.tensorflow, "_nerdl", NerDLModel.tfFile)
  }
}

trait ReadsNERGraph extends ParamsAndFeaturesReadable[NerDLModel] with ReadTensorflowModel {

  override val tfFile = "tensorflow"

  def readNerGraph(instance: NerDLModel, path: String, spark: SparkSession): Unit = {
    val tf = readTensorflowModel(path, spark, "_nerdl")
    instance.setTensorflow(tf)
    instance.getModelIfNotSet
  }

  addReader(readNerGraph)
}

trait PretrainedNerDL {
  def pretrained(name: String = "ner_precise", language: Option[String] = Some("en"), remoteLoc: String = ResourceDownloader.publicLoc): NerDLModel =
    ResourceDownloader.downloadModel(NerDLModel, name, language, remoteLoc)
}


object NerDLModel extends ParamsAndFeaturesReadable[NerDLModel] with ReadsNERGraph with PretrainedNerDL
