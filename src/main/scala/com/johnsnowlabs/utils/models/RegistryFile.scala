package com.johnsnowlabs.utils.models

case class RegistryModel(
                        val modelName: String,
                        val modelType: String,
                        val modelVersion: String,
                        )

case class RegistryFile(
                       val modelList: Seq[RegistryFile]
                       )
