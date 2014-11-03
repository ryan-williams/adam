package org.bdgenomics.adam.cli

import java.io.File

import org.apache.commons.io.FileUtils
import org.apache.hadoop.mapreduce.Job
import org.bdgenomics.adam.util.SparkFunSuite
import org.bdgenomics.adam.rdd.ADAMContext._

class ViewSuite extends SparkFunSuite {
  sparkTest("applies filters") {

    FileUtils.deleteDirectory(new File("flag-values.adam"))

    val transform =
      new Transform(Args4j[TransformArgs](
        Array(ClassLoader.getSystemClassLoader.getResource("flag-values.sam").getFile, "flag-values.adam")
      ))

    transform.run(sc, new Job())

    val view = new View(Args4j[ViewArgs]("-f 4 flag-values.adam flag-values-unmapped.adam".split(" ")))
    view.run(sc, new Job())

    assert(sc.adamLoad("flag-values-unmapped.sam").count() == 2048)
  }
}
