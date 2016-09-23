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

import com.google.api.services.dataproc.Dataproc;
import com.google.api.services.dataproc.model.Operation;
import com.google.api.services.storage.Storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.MoreObjects.firstNonNull;
import static java.util.Arrays.asList;

class DataprocClient {

  private static final Logger LOG = LoggerFactory.getLogger(DataprocClient.class);

  private final Dataproc dataproc;
  private final Storage storage;

  private final Clusters clusters;
  private final Jobs jobs;

  private final String projectId;
  private final String clusterId;
  private final Map<String, String> clusterProperties;

  DataprocClient(Dataproc dataproc, Storage storage, String projectId, String clusterId,
                 Map<String, String> clusterProperties) {
    this.dataproc = dataproc;
    this.storage = storage;

    this.projectId = projectId;
    this.clusterId = clusterId;
    this.clusterProperties = clusterProperties;

    this.clusters = new Clusters(this);
    this.jobs = new Jobs(this);
  }

  String getProjectId() {
    return projectId;
  }

  String getRegion() {
    return "global";
  }

  String getClusterId() {
    return clusterId;
  }

  Clusters clusters() {
    return clusters;
  }

  Dataproc dataproc() {
    return dataproc;
  }

  Storage storage() {
    return storage;
  }

  JobStatus submit(Job job) throws IOException {
    LOG.info("Submitting job to project:{} cluster:{}", projectId, clusterId);

    final Map<String, String> properties = new LinkedHashMap<>();
    properties.putAll(clusterProperties);
    properties.putAll(job.getProperties());
    properties.forEach((key, value) -> LOG.debug("Property {} = {}", key, value));

    final List<String> shippedJars = asList(firstNonNull(job.getShippedJars(), new String[0]));
    final List<String> shippedFiles = asList(firstNonNull(job.getShippedFiles(), new String[0]));

    job.getProperties();
    final com.google.api.services.dataproc.model.Job submittedJob =
        jobs.submit(
            job.getMainClass(),
            job.getJarPath(),
            properties,
            shippedJars,
            shippedFiles,
            job.getArgs());
    final String jobId = submittedJob.getReference().getJobId();

    final com.google.api.services.dataproc.model.JobStatus status;
    try {
      status = jobs.stream(jobId, System.err);
    } catch (InterruptedException | ExecutionException e) {
      throw new IOException(e);
    }

    return new JobStatus("DONE".equals(status.getState()));
  }

  Operation waitFor(Operation op) throws IOException {
    return waitFor(op, 3000L);
  }

  Operation waitFor(Operation op, Long interval) throws IOException {
    while (op.getDone() == null || !op.getDone()) {
      Dataproc.Projects.Regions.Operations.Get opGet =
          dataproc().projects().regions().operations().get(op.getName());
      op = opGet.execute();
      try {
        Thread.sleep(interval);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
    return op;
  }
}
