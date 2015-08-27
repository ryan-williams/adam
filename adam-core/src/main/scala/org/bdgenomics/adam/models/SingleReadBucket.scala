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
package org.bdgenomics.adam.models

import com.esotericsoftware.kryo.{ Kryo, Serializer }
import com.esotericsoftware.kryo.io.{ Output, Input }
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD
import org.bdgenomics.adam.rdd.ADAMContext._
import org.bdgenomics.adam.serialization.AvroSerializer
import org.bdgenomics.formats.avro.{
  AlignmentRecord,
  Fragment,
  Sequence
}
import scala.collection.JavaConversions._

object SingleReadBucket extends Logging {
  def apply(rdd: RDD[AlignmentRecord]): RDD[SingleReadBucket] = {
    rdd.groupBy(p => (p.getRecordGroupName, p.getReadName))
      .map(kv => {
        val (_, reads) = kv

        // split by mapping
        val (mapped, unmapped) = reads.partition(_.getReadMapped)
        val (primaryMapped, secondaryMapped) = mapped.partition(_.getPrimaryAlignment)

        // TODO: consider doing validation here (e.g. read says mate mapped but it doesn't exist)
        new SingleReadBucket(primaryMapped, secondaryMapped, unmapped)
      })
  }
}

case class SingleReadBucket(primaryMapped: Iterable[AlignmentRecord] = Seq.empty,
                            secondaryMapped: Iterable[AlignmentRecord] = Seq.empty,
                            unmapped: Iterable[AlignmentRecord] = Seq.empty) {
  // Note: not a val in order to save serialization/memory cost
  def allReads = {
    primaryMapped ++ secondaryMapped ++ unmapped
  }

  def toFragment: Fragment = {
    // take union of all reads, as we will need this for building and
    // want to pay the cost exactly once
    val unionReads = allReads

    // do we have any chimeric reads?
    val sequences: Seq[Sequence] = if (secondaryMapped.exists(_.getSupplementaryAlignment)) {
      // group by read id
      unionReads.groupBy(_.getReadNum)
        .toSeq
        .sortBy(_._1)
        .map(p => {
          val (_, reads) = p

          // first, do we have a whole unclipped read here? if we do, then we
          // can skip the rest of this process
          val unclippedRead = reads.find(r => {
            r.getBasesTrimmedFromStart == 0 &&
              r.getBasesTrimmedFromEnd == 0
          })

          // do we have the read? if so, return it, else, fun times
          unclippedRead.fold({
            // sort by number of bases clipped from the start
            val tempReadsSorted = reads.toSeq
              .sortBy(_.getBasesTrimmedFromStart)
            val endIdx = tempReadsSorted.indexWhere(_.getBasesTrimmedFromEnd == 0)
            val readsSorted = tempReadsSorted.take(endIdx + 1)

            val (sequence, qual) = readsSorted.map(r => {
              if (r.getReadMapped && r.getReadNegativeStrand) {
                (Alphabet.dna.reverseComplement(r.getSequence,
                  (c: Char) => Symbol('N', 'N')),
                  r.getQual.reverse)
              } else {
                (r.getSequence, r.getQual)
              }
            }).reduce((v1: (String, String), v2: (String, String)) => (v1._1 + v2._1, v1._2 + v2._2))

            Sequence.newBuilder()
              .setBases(sequence)
              .setQualities(qual)
              .build()
          })(r => {
            if (r.getReadMapped && r.getReadNegativeStrand) {
              Sequence.newBuilder()
                .setBases(Alphabet.dna.reverseComplement(r.getSequence,
                  (c: Char) => Symbol('N', 'N')))
                .setQualities(r.getQual.reverse)
                .build()
            } else {
              Sequence.newBuilder()
                .setBases(r.getSequence)
                .setQualities(r.getQual)
                .build()
            }
          })
        })
    } else {
      // get the read sequence per read in the fragment
      (primaryMapped ++ unmapped)
        .map(r => {
          val s = if (r.getReadMapped && r.getReadNegativeStrand) {
            Sequence.newBuilder()
              .setBases(Alphabet.dna.reverseComplement(r.getSequence, (c: Char) => Symbol('N', 'N')))
              .setQualities(r.getQual.reverse)
              .build()
          } else {
            Sequence.newBuilder()
              .setBases(r.getSequence)
              .setQualities(r.getQual)
              .build()
          }
          (r.getReadNum, s)
        }).toSeq
        .sortBy(_._1)
        .map(_._2)
    }

    // start building fragment
    val builder = Fragment.newBuilder()
      .setReadName(unionReads.head.getReadName)
      .setSequences(seqAsJavaList(sequences))
      .setAlignments(seqAsJavaList(allReads.filter(_.getReadMapped).map(r => {
        r.setSequence(null)
        r.setQual(null)
        r
      }).toSeq))

    // is an insert size defined for this fragment?
    primaryMapped.headOption
      .foreach(r => {
        Option(r.getInferredInsertSize).foreach(is => {
          builder.setFragmentSize(is.toInt)
        })
      })

    // set platform unit, if known
    Option(unionReads.head.getRecordGroupPlatformUnit)
      .foreach(p => builder.setInstrument(p))

    // set record group name, if known
    Option(unionReads.head.getRecordGroupName)
      .foreach(n => builder.setRunId(n))

    builder.build()
  }
}

class SingleReadBucketSerializer extends Serializer[SingleReadBucket] {
  val recordSerializer = new AvroSerializer[AlignmentRecord]()

  def writeArray(kryo: Kryo, output: Output, reads: Seq[AlignmentRecord]): Unit = {
    output.writeInt(reads.size, true)
    for (read <- reads) {
      recordSerializer.write(kryo, output, read)
    }
  }

  def readArray(kryo: Kryo, input: Input): Seq[AlignmentRecord] = {
    val numReads = input.readInt(true)
    (0 until numReads).foldLeft(List[AlignmentRecord]()) {
      (a, b) => recordSerializer.read(kryo, input, classOf[AlignmentRecord]) :: a
    }
  }

  def write(kryo: Kryo, output: Output, groupedReads: SingleReadBucket) = {
    writeArray(kryo, output, groupedReads.primaryMapped.toSeq)
    writeArray(kryo, output, groupedReads.secondaryMapped.toSeq)
    writeArray(kryo, output, groupedReads.unmapped.toSeq)
  }

  def read(kryo: Kryo, input: Input, klazz: Class[SingleReadBucket]): SingleReadBucket = {
    val primaryReads = readArray(kryo, input)
    val secondaryReads = readArray(kryo, input)
    val unmappedReads = readArray(kryo, input)
    new SingleReadBucket(primaryReads, secondaryReads, unmappedReads)
  }
}

