package com.johnsnowlabs.util

import java.io.{File, InputStream}
import java.nio.file.{Files, Paths}
import java.util.zip.ZipInputStream

import org.rauschig.jarchivelib.{ArchiveFormat, Archiver, ArchiverFactory, CompressionType}

package object compress {
  def unzipUntarInplace(path: String): Unit = {
    if (Files.probeContentType(Paths.get(path)) == "application/gzip") {
      val renamedFileName: String = s"$path.tar.gz"
      Files.move(Paths.get(path), Paths.get(renamedFileName))
      unzipFile(renamedFileName, path)
    }
  }

  def unzipFile(zipFile: String, outDir: String): Unit = {
    val archiver: Archiver = ArchiverFactory.createArchiver(ArchiveFormat.TAR, CompressionType.GZIP)
    archiver.extract(new File(zipFile), new File(outDir))
  }
}
