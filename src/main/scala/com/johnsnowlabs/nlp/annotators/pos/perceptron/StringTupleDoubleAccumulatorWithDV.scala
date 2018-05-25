package com.johnsnowlabs.nlp.annotators.pos.perceptron

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.{Map => MMap}

class StringTupleDoubleAccumulatorWithDV(defaultMap: MMap[(String, String), Double] = MMap.empty[(String, String), Double])
  extends AccumulatorV2[((String, String), Double), Map[(String, String), Double]] {

  private val mmap = defaultMap.withDefaultValue(0.0)

  override def reset(): Unit = mmap.clear()

  override def add(v: ((String, String), Double)): Unit = mmap(v._1) += v._2

  override def value: Map[(String, String), Double] = mmap.toMap

  override def copy(): AccumulatorV2[((String, String), Double), Map[(String, String), Double]] =
    new StringTupleDoubleAccumulatorWithDV(MMap[(String, String), Double](value.toSeq:_*).withDefaultValue(0.0))

  override def isZero: Boolean = mmap.isEmpty

  override def merge(other: AccumulatorV2[((String, String), Double), Map[(String, String), Double]]): Unit =
    other.value.foreach{case (k, v) => mmap(k) += v}
}

class StringMapStringDoubleAccumulatorWithDVMutable(defaultMap: MMap[String, MMap[String, Double]] = MMap.empty[String, MMap[String, Double]])
  extends AccumulatorV2[(String, MMap[String, Double]), MMap[String, MMap[String, Double]]] {

  private val mmap = defaultMap.withDefaultValue(MMap.empty[String, Double].withDefaultValue(0.0))

  override def reset(): Unit = mmap.clear()

  override def add(v: (String, MMap[String, Double])): Unit = {
    mmap(v._1) = mmap(v._1) ++ v._2
  }

  def update(v: (String, MMap[String, Double])): Unit = {
    mmap(v._1) = mmap(v._1) ++ v._2
  }

  def innerSet(k: (String, String), v: Double): Unit = {
    mmap(k._1) = mmap(k._1) ++ MMap(k._2 -> v)
  }

  override def value: MMap[String, MMap[String, Double]] = mmap

  override def copy(): AccumulatorV2[(String, MMap[String, Double]), MMap[String, MMap[String, Double]]] =
    new StringMapStringDoubleAccumulatorWithDVMutable(MMap[String, MMap[String, Double]](value.toSeq:_*).withDefaultValue(MMap.empty[String, Double].withDefaultValue(0.0)))

  override def isZero: Boolean = mmap.isEmpty

  override def merge(other: AccumulatorV2[(String, MMap[String, Double]), MMap[String, MMap[String, Double]]]): Unit =
    other.value.foreach{case (k, v) => v.foreach{case (kk, vv) =>
        mmap(k) = mmap(k) ++ MMap(kk -> vv)
    }}
}

