package com.johnsnowlabs.utils.models

import com.johnsnowlabs.util.resolvers.commons.SemVer

trait ResourceConsumer extends ResourceConsumerParams {
  def getModelName = get(rModelName)

  def getModelVersion = get(rModelVersion)

  def getModelResolver = get(modelResolver)

  def isRequestingResolver: Boolean = getModelName.isDefined

  def getOnlineModel: OnlineModel = OnlineModel(
    getModelName.get,
    SemVer(getModelVersion.getOrElse("0.0.0")),
    "annotatorType"
  )
}