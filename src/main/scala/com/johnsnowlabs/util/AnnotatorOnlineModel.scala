package com.johnsnowlabs.util

import com.johnsnowlabs.util.resolvers.commons.SemVer

class AnnotatorOnlineModel(
                          var _name: String = "",
                          var _modelType: String = "",
                          var _version: SemVer = SemVer("0.0.0")
                          ) extends AnnotatorResource[AnnotatorOnlineModel] {

}
