// Copyright (c) 2014, Cloudera, inc.
// Confidential Cloudera Information: Covered by NDA.
package org.kududb.mapreduce;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Small committer class that does not do anything.
 */
public class KuduTableOutputCommitter extends OutputCommitter {
  @Override
  public void setupJob(JobContext jobContext) throws IOException {

  }

  @Override
  public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {

  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
    return false;
  }

  @Override
  public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {

  }

  @Override
  public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {

  }
}
