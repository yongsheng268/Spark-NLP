package com.johnsnowlabs.nlp

object AnnotatorType extends Enumeration {
  type AnnotatorType = Value
  val DOCUMENT = Value("document")
  val TOKEN = Value("token")
  val DATE = Value("date")
  val ENTITY = Value("entity")
  val REGEX = Value("regex")
  val SPELL = Value("spell")
  val SENTIMENT = Value("sentiment")
  val POS = Value("pos")
  val NAMED_ENTITY = Value("named_entity")
  val NEGEX = Value("negex")
  val DEPENDENCY = Value("dependency")
  val DUMMY = Value("dummy")
  implicit def at2str(v: AnnotatorType) = v.toString
}

