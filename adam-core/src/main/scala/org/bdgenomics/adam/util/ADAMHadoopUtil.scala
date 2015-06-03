package org.bdgenomics.adam.util

import org.apache.hadoop.mapreduce.TaskAttemptContext

object ADAMHadoopUtil {

  def setTaskAttemptContextStatus(context: TaskAttemptContext, status: String): Unit = {
    val method = context.getClass.getMethod("setStatus", classOf[java.lang.String])
    method.invoke(context, status: java.lang.String)
  }

}
