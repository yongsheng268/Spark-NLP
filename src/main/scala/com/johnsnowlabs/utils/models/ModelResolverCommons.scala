package com.johnsnowlabs.utils.models

import com.johnsnowlabs.util.resolvers.commons.SemVer


case class OnlineModel(
                        modelName: String,
                        modelVersion: SemVer,
                        modelType: String
                      )

case class DownloadedModel (
                           path: String
                           )