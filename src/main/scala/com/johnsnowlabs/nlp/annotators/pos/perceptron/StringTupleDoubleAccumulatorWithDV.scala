package com.johnsnowlabs.nlp.annotators.pos.perceptron

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable.{Map => MMap}

class StringTupleDoubleAccumulatorWithDV(defaultMap: MMap[(String, String), Double] = MMap.empty[(String, String), Double])
  extends AccumulatorV2[((String, String), Double), Map[(String, String), Double]] {

  private var mmap = defaultMap.withDefaultValue(0.0)

  override def reset(): Unit = mmap.clear()

  override def add(v: ((String, String), Double)): Unit = mmap(v._1) += v._2

  override def value: Map[(String, String), Double] = mmap.toMap

  override def copy(): AccumulatorV2[((String, String), Double), Map[(String, String), Double]] = {
    val c = new StringTupleDoubleAccumulatorWithDV(MMap.empty[(String, String), Double])
    c.mmap = this.mmap
    c
  }

  override def isZero: Boolean = mmap.isEmpty

  override def merge(other: AccumulatorV2[((String, String), Double), Map[(String, String), Double]]): Unit =
    mmap ++= other.value
}

class StringMapStringDoubleAccumulatorWithDV(defaultMap: MMap[String, MMap[String, Double]] = MMap.empty[String, MMap[String, Double]])
  extends AccumulatorV2[(String, MMap[String, Double]), Map[String, Map[String, Double]]] {

  private var mmap = defaultMap.withDefaultValue(MMap.empty[String, Double].withDefaultValue(0.0))

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

  override def value: Map[String, Map[String, Double]] = mmap.mapValues(_.toMap).toMap
    .withDefaultValue(Map.empty[String, Double].withDefaultValue(0.0))

  override def copy(): AccumulatorV2[(String, MMap[String, Double]), Map[String, Map[String, Double]]] = {
    val c = new StringMapStringDoubleAccumulatorWithDV(MMap.empty[String, MMap[String, Double]])
    c.mmap = this.mmap
    c
  }

  override def isZero: Boolean = mmap.isEmpty

  override def merge(other: AccumulatorV2[(String, MMap[String, Double]), Map[String, Map[String, Double]]]): Unit =
    other.value.foreach{case (k, v) => v.foreach{case (kk, vv) =>
        mmap(k) = mmap(k) ++ MMap(kk -> vv)
    }}
}

