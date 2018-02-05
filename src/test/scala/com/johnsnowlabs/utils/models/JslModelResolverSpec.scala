package com.johnsnowlabs.utils.models

import org.scalatest.{FlatSpec, Matchers}
import com.johnsnowlabs.nlp.util.ConfigHelper
import scala.language.reflectiveCalls

class JslModelResolverSpec extends FlatSpec with Matchers {

  def fixture = new {
    val m = new JslModelResolver()
  }

  def badRepoFixture = new {
    val m = new JslModelResolver("")
  }

  "A JslModelResolverSpec" should "retrieve the registry file" in {
//    val f = fixture
//    assert(f.m.getRegistry.get.length > 0, "Registry is empty")
  }

  it should "throw an exception when the file is not found" in {
//    val f = badRepoFixture
//    assert(f.m.getRegistry.get.length < 0, "Registry is empty")
  }
}
