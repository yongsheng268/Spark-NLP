package com.johnsnowlabs.utils.models

import org.scalatest.{FlatSpec, Matchers}

class ModelResolverCommonsSpec extends FlatSpec with Matchers {
  "A ModelVersion" should "parse a version string" in {
    val version = "1.2.3"
    val modelVersion = ModelVersion(version)
    assert(modelVersion.major == 1 && modelVersion.minor == 2 && modelVersion.patch == 3)
  }

  it should "parse incomplete version numbers" in {
    val modelVersionOne = ModelVersion("1.2")
    val modelVersionTwo = ModelVersion("1")

    assert(modelVersionOne.major == 1 && modelVersionOne.minor == 2 && modelVersionOne.patch == 0)
    assert(modelVersionTwo.major == 1 && modelVersionTwo.minor == 0 && modelVersionTwo.patch == 0)
  }

  it should "assign default version 0.0.0 to wrong version strings" in {
    val invalidVersion = "1.1.1.1"
    val modelVersion = ModelVersion(invalidVersion)

    assert(modelVersion.major == 0 && modelVersion.minor == 0 && modelVersion.patch == 0)
  }

  it should "compare to other versions" in {
    val version = "4.0.1"
    val v1 = ModelVersion(version)
    val lowers = ModelVersion("4.0.0") :: (for {
      major <- 0 until 4
      minor <- 0 until 10
      patch <- 0 until 100
    } yield ModelVersion(major, minor, patch)).toList

    val greaters = (for {
      major <- 4 until 10
      minor <- 0 until 10
      patch <- 0 until 100
    } yield ModelVersion(major, minor, patch)).toList.tail.tail

    lowers.foreach  { other: ModelVersion => assert( v1 > other, "Should be lower" ) }
    greaters.foreach { other: ModelVersion => assert( v1 < other, "Should be equal") }
    assert(v1 == ModelVersion(version), s"$v1 should be equal to ${ModelVersion(version)}")
    (ModelVersion(version) :: lowers).foreach { other: ModelVersion => assert( v1 >= other, "Should be lower or equal" ) }
    (ModelVersion(version) :: greaters).foreach { other: ModelVersion => assert( v1 <= other, "Should be greater or equal") }
  }
}
