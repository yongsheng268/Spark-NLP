package com.johnsnowlabs.utils.models

abstract class ModelResolver {
  def getRegistry

  def parseRegistry: RegistryFile
}
