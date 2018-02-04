package com.johnsnowlabs.util.resolvers.commons

case class SemVer(
                         major: Int,
                         minor: Int,
                         patch: Int
                       ) {
  def this(vNumbers: Int*) = this(vNumbers(0), vNumbers(1), vNumbers(2))
  def this(major: Int, minor: Int) = this(major, minor, 0)
  def this(major: Int) = this(major, 0, 0)
  def this() = this(0, 0, 0)
  def this(major: String, minor: String, patch: String) = this(major.toInt, minor.toInt, patch.toInt)
  def this(major: String, minor: String) = this(major.toInt, minor.toInt, 0)
  def this(ver: String) =
    this( (ver.split('.').map { _.toInt } match {
      case Array(major: Int, minor: Int, patch: Int) => Seq(major, minor, patch)
      case Array(major: Int, minor: Int) => Seq(major, minor, 0)
      case Array(major: Int) => Seq(major, 0, 0)
      case _ => Seq(0, 0, 0)
    }): _*)

  def versionAsString: String = s"${major.toString}.${minor.toString}.${patch.toString}"

  def == (other: String): Boolean = this == SemVer(other)
  def == (other: SemVer): Boolean =
    major == other.major && minor == other.minor && patch == other.patch
  def < (other: SemVer): Boolean =
    major < other.major || major == other.major && minor < other.minor || major == other.major && minor == other.minor && patch < other.patch
  def > (other: SemVer): Boolean =
    major > other.major || major == other.major && minor > other.minor || major == other.major && minor == other.minor && patch > other.patch
  def <= (other: SemVer): Boolean =
    this < other || this == other
  def >= (other: SemVer): Boolean =
    this > other || this == other

  override def toString: String = versionAsString
}

object SemVer {
  def apply(major: Int, minor: Int) = new SemVer(major, minor, 0)
  def apply(major: Int) = new SemVer(major, 0)
  def apply(major: String, minor: String, patch: String) = new SemVer(major, minor, patch)
  def apply(major: String, minor: String) = new SemVer(major, minor)
  def apply(ver: String) = new SemVer(ver)
}