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

    Builder withProperty(String key, String value);

    Builder withProperties(Map<String, String> properties);

    Builder withCredentialProvider(CredentialProvider credentialProvider);

    DataprocHadoopRunner build();
  }
}
