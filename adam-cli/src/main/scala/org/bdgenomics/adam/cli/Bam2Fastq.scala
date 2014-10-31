/**
 * Licensed to Big Data Genomics (BDG) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The BDG licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.bdgenomics.adam.cli

import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.projections.{ AlignmentRecordField, Projection }
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.formats.avro.AlignmentRecord
import org.kohsuke.args4j.{ Option, Argument }
import org.bdgenomics.adam.rdd.read.AlignmentRecordContext._

class Bam2FastqArgs extends ParquetLoadSaveArgs {
  @Argument(required = false, metaVar = "OUTPUT", usage = "When writing FASTQ data, all second-in-pair reads will go here, if this argument is provided", index = 2)
  var outputPath2: String = null
  @Option(required = false, name = "-check_pairs", usage = "When writing paired FASTQ data to two files, set this to true to verify that all reads are paired.")
  var enforceFastQPairing: Boolean = false
}

object Bam2Fastq extends ADAMCommandCompanion {
  override val commandName = "bam2fastq"
  override val commandDescription = "Convert BAM to FASTQ files"

  override def apply(cmdLine: Array[String]): Bam2Fastq =
    new Bam2Fastq(Args4j[Bam2FastqArgs](cmdLine))
}

class Bam2Fastq(val args: Bam2FastqArgs) extends ADAMSparkCommand[Bam2FastqArgs] {
  override val companion = Bam2Fastq

  override def run(sc: SparkContext, job: Job): Unit = {

    val projection = Projection(
      AlignmentRecordField.readName,
      AlignmentRecordField.sequence,
      AlignmentRecordField.origQual
    )

    val reads: RDD[AlignmentRecord] = sc.adamLoad(args.inputPath, projection = Some(projection))

    reads.adamSaveAsPairedFastq(args.outputPath, args.outputPath2, enforceAllReadsPaired = args.enforceFastQPairing)
  }

}
