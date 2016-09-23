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

import java.io.IOException;
import java.util.Map;

/**
 * Interface for submitting jobs to a Google Cloud Dataproc cluster.
 *
 * Use {@link #builder(String, String)} to configure and instance.
 */
public interface DataprocHadoopRunner {

  static Builder builder(String projectId, String clusterId) {
    return new DataprocHadoopRunnerImpl.Builder(projectId, clusterId);
  }

  /**
   * Submit a {@link Job} and wait for it to complete.
   *
   * @param job  The job to submit
   * @return a {@link JobStatus} for the completed job
   * @throws IOException If the job failed to submit
   */
  JobStatus submit(Job job) throws IOException;

  interface Builder {

    /**
     * Specify a property key-value pair that will be set on all jobs.
     *
     * @param key    Property key
     * @param value  Property value
     * @return This builder
     */
    Builder withProperty(String key, String value);

    /**
     * Specify property key-value pairs that will be set on all jobs.
     *
     * @param properties  Properties map
     * @return This builder
     */
    Builder withProperties(Map<String, String> properties);

    /**
     *  Specify which {@link CredentialProvider} to use when interacting with Google APIs.
     *
     * @param credentialProvider  The credential provider to use
     * @return This builder
     */
    Builder withCredentialProvider(CredentialProvider credentialProvider);

    /**
     * Create a {@link DataprocHadoopRunner} with configuration specified by this builder.
     *
     * @return A new instance of DataprocHadoopRunner
     */
    DataprocHadoopRunner build();
  }
}
