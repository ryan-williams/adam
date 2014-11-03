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

package org.bdgenomics.adam.predicates

import org.bdgenomics.adam.projections.AlignmentRecordField
import org.bdgenomics.formats.avro.AlignmentRecord

object AlignmentRecordConditions {

  val bitToField: Map[Int, (AlignmentRecordField.Value, Boolean)] = Map(
    0x1 -> (AlignmentRecordField.readPaired, true),
    0x2 -> (AlignmentRecordField.properPair, true),
    0x4 -> (AlignmentRecordField.readMapped, false),
    0x8 -> (AlignmentRecordField.mateMapped, false),
    0x10 -> (AlignmentRecordField.readNegativeStrand, true),
    0x20 -> (AlignmentRecordField.mateNegativeStrand, true),
    0x40 -> (AlignmentRecordField.firstOfPair, true),
    0x80 -> (AlignmentRecordField.secondOfPair, false),
    0x100 -> (AlignmentRecordField.primaryAlignment, false),
    0x200 -> (AlignmentRecordField.failedVendorQualityChecks, true),
    0x400 -> (AlignmentRecordField.duplicateRead, true),
    0x800 -> (AlignmentRecordField.supplmentaryAlignment, true)
  )

  def conditionsForBits(n: Int, expectedValue: Boolean = true): RecordCondition[AlignmentRecord] = {
    RecordCondition(bitToField.map {
      case (bit, (field, value)) => FieldCondition(field, ((n & bit) > 0) == expectedValue)
    }.toSeq: _*)
  }

  def apply(field: AlignmentRecordField.Value, bool: Boolean = true): RecordCondition[AlignmentRecord] = {
    RecordCondition[AlignmentRecord](FieldCondition(field, bool))
  }

  def apply(field: AlignmentRecordField.Value, filter: Int => Boolean): RecordCondition[AlignmentRecord] = {
    RecordCondition[AlignmentRecord](FieldCondition(field.toString, filter))
  }

  val isMapped = apply(AlignmentRecordField.readMapped)
  val isUnique = apply(AlignmentRecordField.duplicateRead, false)

  val isPrimaryAlignment = apply(AlignmentRecordField.primaryAlignment)

  val passedVendorQualityChecks = apply(AlignmentRecordField.failedVendorQualityChecks, false)

  def isHighQuality(minQuality: Int) = apply(AlignmentRecordField.mapq, (x: Int) => x > minQuality)

}
