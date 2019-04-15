package com.johnsnowlabs.ml.tensorflow

import java.lang.reflect.Modifier

import com.johnsnowlabs.ml.tensorflow.TensorResources.extractFloats
import com.johnsnowlabs.nlp.annotators.ner.Verbose

class TensorflowSpell(
  val tensorflow: TensorflowWrapper,
  val verboseLevel: Verbose.Value
  ) extends Logging with Serializable {

  val testInitOp = "test/init"
  val validWords = "valid_words"
  val fileNameTest = "file_name"
  val inMemoryInput = "in-memory-input"
  val batchesKey = "batches"
  val lossKey = "Add:0"
  val dropoutRate = "dropout_rate"

  /* returns the loss associated with the last word, given previous history  */
  def predict(dataset: Array[Array[Int]], cids: Array[Array[Int]], cwids:Array[Array[Int]]) = {

    val packed = dataset.zip(cids).zip(cwids).map {
      case ((_ids, _cids), _cwids) => Array(_ids, _cids, _cwids)
    }

    val tensors = new TensorResources()
    val inputTensor = tensors.createTensor(packed)

    val lossWords = tensorflow.session.runner
      .feed(dropoutRate, tensors.createTensor(1.0f))
      .feed("batches:0", tensors.createTensor(dataset.map(_.dropRight(1))))
      .feed("batches:1", tensors.createTensor(cids.map(_.tail)))
      .feed("batches:2", tensors.createTensor(cwids.map(_.tail)))
      .fetch(lossKey)
      .fetch(validWords)
      .run()

    tensors.clearTensors()

    val result = extractFloats(lossWords.get(0))
    // TODO width should come from somewhere else
    val width = inputTensor.shape()(2)
    result.grouped(width.toInt - 1).map(_.last)

  }
}
