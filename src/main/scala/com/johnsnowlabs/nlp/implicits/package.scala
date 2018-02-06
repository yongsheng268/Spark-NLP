package com.johnsnowlabs.nlp

import com.johnsnowlabs.util.resolvers.DownloadedResource

package object implicits {
  implicit def downloadedResourceToPath[A](resource: DownloadedResource[A]) =
    resource.path
}
