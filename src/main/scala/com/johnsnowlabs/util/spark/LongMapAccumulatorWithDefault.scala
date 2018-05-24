package com.johnsnowlabs.util.spark

import org.apache.spark.util.AccumulatorV2
import scala.collection.mutable.{Map=>MMap}

class LongMapAccumulatorWithDefault(defaultMap: MMap[String, Long] = MMap.empty[String, Long], defaultValue: Long = 0)
  extends AccumulatorV2[(String, Long), Map[String, Long]] {

  private val mmap = defaultMap.withDefaultValue(defaultValue)

  override def reset(): Unit = mmap.clear()

  override def add(v: (String, Long)): Unit = mmap(v._1) += v._2

  override def value: Map[String, Long] = mmap.toMap.withDefaultValue(defaultValue)

  override def copy(): AccumulatorV2[(String, Long), Map[String, Long]] =
    new LongMapAccumulatorWithDefault(MMap[String, Long](value.toSeq:_*), defaultValue)

  override def isZero: Boolean = mmap.isEmpty

  override def merge(other: AccumulatorV2[(String, Long), Map[String, Long]]): Unit =
    other.value.foreach{case (k, v) => mmap(k) += v}
}

class DoubleMapAccumulatorWithDefault(defaultMap: MMap[String, Double] = MMap.empty[String, Double], defaultValue: Double = 0.0)
  extends AccumulatorV2[(String, Double), Map[String, Double]] {

  private val mmap = defaultMap.withDefaultValue(defaultValue)

  override def reset(): Unit = mmap.clear()

  override def add(v: (String, Double)): Unit = mmap(v._1) += v._2

  override def value: Map[String, Double] = mmap.toMap.withDefaultValue(defaultValue)

  override def copy(): AccumulatorV2[(String, Double), Map[String, Double]] =
    new DoubleMapAccumulatorWithDefault(MMap[String, Double](value.toSeq:_*), defaultValue)

  override def isZero: Boolean = mmap.isEmpty

  override def merge(other: AccumulatorV2[(String, Double), Map[String, Double]]): Unit =
    other.value.foreach{case (k, v) => mmap(k) += v}
}

class TupleKeyDoubleMapAccumulatorWithDefault(defaultMap: MMap[(String, String), Long] = MMap.empty[(String, String), Long], defaultValue: Long = 0)
  extends AccumulatorV2[((String, String), Long), Map[(String, String), Long]] {

  private val mmap = defaultMap.withDefaultValue(defaultValue)

  override def reset(): Unit = mmap.clear()

  override def add(v: ((String, String), Long)): Unit = mmap(v._1) += v._2

  def update(k: (String, String), v: Long): Unit =  mmap(k) = v

  override def value: Map[(String, String), Long] = mmap.toMap.withDefaultValue(defaultValue)

  override def copy(): AccumulatorV2[((String, String), Long), Map[(String, String), Long]] =
    new TupleKeyDoubleMapAccumulatorWithDefault(MMap[(String, String), Long](value.toSeq:_*), defaultValue)

  override def isZero: Boolean = mmap.isEmpty

  override def merge(other: AccumulatorV2[((String, String), Long), Map[(String, String), Long]]): Unit =
    other.value.foreach{case (k, v) => mmap(k) += v}
}
