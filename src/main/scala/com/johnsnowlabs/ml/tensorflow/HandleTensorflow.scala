package com.johnsnowlabs.ml.tensorflow

import java.io.File
import java.nio.file.Paths

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkFiles
import org.apache.spark.sql.SparkSession

trait HandleTensorflow[T] {

  @transient @volatile private var tensorflow: TensorflowWrapper = _
  @transient @volatile private var model: T = _
  protected val uid: String

  final protected def getTensorflowIfNotSet: TensorflowWrapper = {
    if (!isTensorflowSet) {
      println("TENSORFLOW IS NULL IN GET TENSORFLOW")
      val fileName = "tensorflow_"+uid
      val target = Paths.get(SparkFiles.getRootDirectory(), fileName).toString
      val path = if (new File(target).exists()) target else SparkFiles.get(fileName)
      setTensorflow(TensorflowWrapper.read(path, loadContrib=true))
    }
    tensorflow
  }

  final protected def getModel: T = {
    if (!isTensorflowSet)
      getTensorflowIfNotSet
    if (!isModelSet)
      throw new Exception(s"Requested a TensorflowModel for $this which has not been set")
    model
  }

  final protected def setModel(model: T): Unit = {
    getTensorflowIfNotSet
    if (!isModelSet) {
      this.model = model
    }
  }

  def setTensorflow(tf: TensorflowWrapper): this.type = {
    this.tensorflow = tf
    this
  }

  def isTensorflowSet: Boolean = tensorflow != null

  def isModelSet: Boolean = model != null

  def getModelIfNotSet: T
}

object HandleTensorflow {

  def sendToCluster(sparkSession: SparkSession, tf: TensorflowWrapper, uid: String): Unit = {
    /** Making this graph available in all nodes */
    val fileName = "tensorflow_"+uid
    val filePath = Paths.get(SparkFiles.getRootDirectory(), fileName).toString
    val destinationScheme = new Path(filePath)
      .getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
      .getScheme
    tf.saveToFile(filePath)
    if (destinationScheme != "file")
      sparkSession.sparkContext.addFile(filePath)
  }

}
