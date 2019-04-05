package com.johnsnowlabs.ml.tensorflow

import java.io.File
import java.nio.file.Paths

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkFiles
import org.apache.spark.ml.param.Params
import org.apache.spark.sql.SparkSession

trait HandleTensorflow[T] extends Params {

  @transient @volatile protected var tensorflow: TensorflowWrapper = null
  @transient @volatile protected var _model: T = null.asInstanceOf[T]

  final protected def getTensorflowIfNotSet: TensorflowWrapper = {
    if (tensorflow == null) {
      println("TENSORFLOW IS NULL IN GET TENSORFLOW")
      val fileName = "tensorflow_"+this.uid
      val target = Paths.get(SparkFiles.getRootDirectory(), fileName).toString
      val path = if (new File(target).exists()) target else SparkFiles.get(fileName)
      setTensorflow(TensorflowWrapper.read(path, loadContrib=true))
    }
    tensorflow
  }

  def setTensorflow(tf: TensorflowWrapper): this.type = {
    this.tensorflow = tf
    this
  }

  def getModelIfNotSet: T
}

object HandleTensorflow {

  def sendToCluster(sparkSession: SparkSession, tf: TensorflowWrapper, uid: String): Unit = {
    /** Making this graph available in all nodes */
    val fileName = "tensorflow_"+uid
    val destinationScheme = new Path(SparkFiles.getRootDirectory())
      .getFileSystem(sparkSession.sparkContext.hadoopConfiguration)
      .getScheme
    tf.saveToFile(Paths.get(SparkFiles.getRootDirectory(), fileName).toString)
    if (destinationScheme != "file")
      sparkSession.sparkContext.addFile(fileName)
  }

}
