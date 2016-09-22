/*
 * -\-\-
 * Dataproc Java Submitter
 * --
 * Copyright (C) 2016 Spotify AB
 * --
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * -/-/-
 */
package com.spotify.dataproc;

import java.io.Serializable;

/**
 * Final status of an executed job.
 */
public class JobStatus implements Serializable {

  private final boolean successful;

  /**
   * Create a {@link JobStatus} with the given execution status
   * @param successful the execution status
   */
  public JobStatus(boolean successful) {
    this.successful = successful;
  }

  /**
   * Return true if the job was successful
   * @return true if job was executed successfully
   */
  public boolean isSuccessful() {
    return successful;
  }

  @Override
  public String toString() {
    return "JobStatus{" +
        "successful=" + successful +
        '}';
  }
}
