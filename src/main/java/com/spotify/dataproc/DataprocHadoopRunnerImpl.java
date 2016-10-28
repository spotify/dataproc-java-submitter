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

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.dataproc.Dataproc;
import com.google.api.services.storage.Storage;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.annotation.Nullable;

class DataprocHadoopRunnerImpl implements DataprocHadoopRunner {

  private final DataprocClient dataprocClient;

  private DataprocHadoopRunnerImpl(DataprocClient dataprocClient) {
    this.dataprocClient = dataprocClient;
  }

  @Override
  public JobStatus submit(Job job) throws IOException {
    return dataprocClient.submit(job);
  }

  public static final class Builder implements DataprocHadoopRunner.Builder {

    private static final String APPLICATION_NAME = "dataproc-hadoop-runner";
    private static final List<String> SCOPES = ImmutableList.of(
        "https://www.googleapis.com/auth/cloud-platform");

    private final String projectId;
    private final String clusterId;

    private final Map<String, String> clusterProperties = new LinkedHashMap<>();

    @Nullable
    private CredentialProvider credentialProvider;

    Builder(String projectId, String clusterId) {
      this.projectId = projectId;
      this.clusterId = clusterId;
    }

    @Override
    public DataprocHadoopRunner.Builder withProperty(String key, String value) {
      clusterProperties.put(key, value);
      return this;
    }

    @Override
    public DataprocHadoopRunner.Builder withProperties(Map<String, String> properties) {
      clusterProperties.putAll(properties);
      return this;
    }

    @Override
    public DataprocHadoopRunner.Builder withCredentialProvider(CredentialProvider credentialProvider) {
      this.credentialProvider = credentialProvider;
      return this;
    }

    @Override
    public DataprocHadoopRunner build() {
      CredentialProvider credentialProvider = Optional.ofNullable(this.credentialProvider)
          .orElseGet(DefaultCredentialProvider::new);

      try {
        final HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
        final JsonFactory jsonFactory = new JacksonFactory();

        final GoogleCredential credentials = credentialProvider.getCredential(SCOPES);
        final Dataproc dataproc = new Dataproc.Builder(httpTransport, jsonFactory, credentials)
            .setApplicationName(APPLICATION_NAME).build();
        final Storage storage = new Storage.Builder(httpTransport, jsonFactory, credentials)
            .setApplicationName(APPLICATION_NAME).build();

        final DataprocClient dataprocClient =
            new DataprocClient(dataproc, storage, projectId, clusterId, clusterProperties);

        return new DataprocHadoopRunnerImpl(dataprocClient);
      } catch (Throwable e) {
        throw Throwables.propagate(e);
      }
    }
  }
}
