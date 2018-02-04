package com.johnsnowlabs.util

import com.johnsnowlabs.util.resolvers.commons.SemVer

trait AnnotatorResource[T] {
  var _name: String
  var _version: SemVer

  def name: String = _name
  def version: SemVer = _version

  def setName(name: String): this.type = {
    _name = name
    this
  }

  def setVersion(ver: SemVer): this.type = {
    _version = ver
    this
  }

  def setVersion(ver: String): this.type = setVersion(SemVer(ver))
}
