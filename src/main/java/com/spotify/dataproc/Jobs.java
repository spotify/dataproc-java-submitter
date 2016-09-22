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

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpStatusCodes;
import com.google.api.client.http.InputStreamContent;
import com.google.api.services.dataproc.model.Cluster;
import com.google.api.services.dataproc.model.HadoopJob;
import com.google.api.services.dataproc.model.Job;
import com.google.api.services.dataproc.model.JobPlacement;
import com.google.api.services.dataproc.model.JobStatus;
import com.google.api.services.dataproc.model.SubmitJobRequest;
import com.google.api.services.storage.model.StorageObject;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.Sets.newHashSet;

class Jobs {

  private static final Logger LOG = LoggerFactory.getLogger(Jobs.class);
  private ForkJoinPool FJP = new ForkJoinPool(16);

  private static final Set<String> TERMINAL_JOB_STATES = newHashSet(
      "CANCELLED", "DONE", "ERROR"
  );

  private static final String GCS_STAGING_PREFIX = "google-cloud-dataproc-staging";

  private final DataprocClient client;

  Jobs(DataprocClient client) {
    this.client = client;
  }

  Job submit(
      Optional<String> mainClass,
      Optional<String> mainJarFileUri,
      Map<String, String> properties,
      List<String> jarFileUris,
      List<String> fileUris,
      List<String> args) throws IOException {

    if (!mainClass.isPresent() && !mainJarFileUri.isPresent()) {
      throw new IllegalArgumentException("At least one of MainJarFile and MainClass must be specified");
    }
    if (mainClass.isPresent() && mainJarFileUri.isPresent()) {
      throw new IllegalArgumentException("MainJarFile and MainClass can not be specified at the same time");
    }

    final Cluster cluster = client.clusters().get(client.getClusterId());
    final HadoopJob hadoopJob = new HadoopJob();

    LOG.info("Staging files...");

    if (mainClass.isPresent()) {
      hadoopJob.setMainClass(mainClass.get());
    } else if (mainJarFileUri.isPresent()) {
      final String stagedMainJarFileUri = getStagedURI(cluster, mainJarFileUri.get());
      hadoopJob.setMainJarFileUri(stagedMainJarFileUri);
    }

    final List<String> stagedJarFileUris;
    try {
      stagedJarFileUris = FJP.submit(
          () -> jarFileUris.parallelStream()
              .map(u -> getStagedURI(cluster, u))
              .collect(Collectors.toList()))
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw Throwables.propagate(e);
    }

    final List<String> stagedFileUris;
    try {
      stagedFileUris = FJP.submit(
          () -> fileUris.parallelStream()
              .map(u -> getStagedURI(cluster, u))
              .collect(Collectors.toList()))
          .get();
    } catch (InterruptedException | ExecutionException e) {
      throw Throwables.propagate(e);
    }

    LOG.info("Done staging, submitting job");

    hadoopJob.setProperties(properties);
    hadoopJob.setJarFileUris(stagedJarFileUris);
    hadoopJob.setFileUris(stagedFileUris);
    hadoopJob.setArgs(args);

    final JobPlacement placement = new JobPlacement();
    placement.setClusterName(client.getClusterId());

    final Job job = new Job();
    job.setPlacement(placement);
    job.setHadoopJob(hadoopJob);

    final SubmitJobRequest request = new SubmitJobRequest();
    request.setJob(job);

    return client.dataproc().projects().regions().jobs()
        .submit(client.getProjectId(), client.getRegion(), request)
        .execute();
  }

  private String getStagedURI(Cluster cluster, String uri) {
    URI parsed = URI.create(uri);

    if (!isNullOrEmpty(parsed.getScheme()) && !"file".equals(parsed.getScheme())) {
      return uri;
    }

    // either a file URI or just a path, stage it to GCS
    File local = Paths.get(uri).toFile();
    String configBucket = cluster.getConfig().getConfigBucket();

    LOG.debug("Staging {} in GCS bucket {}", uri, configBucket);

    try {
      InputStreamContent content = new InputStreamContent("application/octet-stream",
                                                          new FileInputStream(local));
      content.setLength(local.length()); // docs say that setting length improves upload perf

      StorageObject object = new StorageObject()
          .setName(Paths.get(GCS_STAGING_PREFIX, local.getName()).toString());

      object = client.storage().objects()
          .insert(configBucket, object, content)
          .execute();
      return new URI("gs", object.getBucket(), "/" + object.getName(), null).toString();
    } catch (URISyntaxException | IOException e) {
      throw Throwables.propagate(e);
    }
  }

  public JobStatus stream(final String jobId, final OutputStream stream)
      throws ExecutionException, InterruptedException {
    final ExecutorService executorService = Executors.newSingleThreadExecutor();

    final JobStatus status;
    try {
      status = executorService.submit(new Callable<JobStatus>() {

        @Override
        public JobStatus call() {
          try {
            return streamAndGetJobStatus();
          } catch (IOException | InterruptedException e) {
            LOG.error("could not read from log stream", e);
            return null;
          }
        }

        public JobStatus streamAndGetJobStatus() throws IOException, InterruptedException {
          StorageObject currentObject = null;
          StorageObject nextObject = null;

          int currentObjectIndex = 0;
          long currentBytesRead = 0;

          Job job;
          do {
            Thread.sleep(1000L);

            job = client.dataproc().projects().regions().jobs()
                .get(client.getProjectId(), client.getRegion(), jobId)
                .execute();

            if (isNullOrEmpty(job.getDriverOutputResourceUri())) {
              continue;
            }

            if (nextObject == null) {
              // check if there is a "next" object
              nextObject = getObject(job.getDriverOutputResourceUri(), currentObjectIndex + 1);
            }

            if ((currentObject == null) || (nextObject != null)) {
              // either (a) we don't have the current object, so we need to fetch it or
              // (b) there is a "next" object, so we ensure the current object is up-to-date
              currentObject = getObject(job.getDriverOutputResourceUri(), currentObjectIndex);
            }

            if (currentObject == null) {
              continue;
            }

            // if there is a current object, read any unread bytes into the output stream
            long unreadBytes = currentObject.getSize().longValue() - currentBytesRead;
            if (unreadBytes > 0) {
              HttpRequest request = client.storage().objects().get(currentObject.getBucket(),
                                                                   currentObject.getName())
                  .setAlt("media").buildHttpRequest();

              request.getHeaders().setRange(currentBytesRead + "-" + (unreadBytes - 1));
              ByteStreams.copy(request.execute().getContent(), stream);

              currentBytesRead += unreadBytes;
            }

            if ((nextObject != null)
                && (currentBytesRead == currentObject.getSize().longValue())) {
              // we have read the entire current object and there is a "next" available
              currentObject = nextObject;
              currentObjectIndex++;
              currentBytesRead = 0;
            }
          } while (!TERMINAL_JOB_STATES.contains(job.getStatus().getState()));

          return job.getStatus();
        }

        private String getObjectUri(String base, int index) {
          return String.format("%s.%09d", base, index);
        }

        private StorageObject getObject(String base, int index) throws IOException {
          final URI uri = URI.create(getObjectUri(base, index));
          final String bucket = uri.getAuthority();
          final String objectName = uri.getPath().substring(1);

          try {
            return client.storage().objects().get(bucket, objectName).execute();
          } catch (GoogleJsonResponseException e) {
            if (HttpStatusCodes.STATUS_CODE_NOT_FOUND == e.getStatusCode()) {
              return null;
            }

            throw e;
          }
        }

      }).get();
    } finally {
      executorService.shutdown();
    }

    return status;
  }

  JobStatus wait(String jobId) throws IOException, InterruptedException {
    while (true) {
      final Job job = client.dataproc().projects().regions().jobs()
          .get(client.getProjectId(), client.getRegion(), jobId)
          .execute();

      if (TERMINAL_JOB_STATES.contains(job.getStatus().getState())) {
        return job.getStatus();
      }

      Thread.sleep(3000L);
    }
  }
}
