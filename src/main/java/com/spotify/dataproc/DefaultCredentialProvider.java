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
import com.google.common.base.Throwables;
import com.google.common.io.Resources;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;

import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.System.getenv;

/**
 * A {@link CredentialProvider} that loads the application default credentials according to
 * {@link GoogleCredential#getApplicationDefault}.
 */
public class DefaultCredentialProvider implements CredentialProvider {

  private static final Logger logger = LoggerFactory.getLogger(DefaultCredentialProvider.class);

  private GoogleCredential credential = null;

  @Override
  public GoogleCredential getCredential(Collection<String> scopes) {
    if (credential == null) {
      try {
        loadCredential();
      } catch (IOException e) {
        logger.error("Failed loading credentials", e);
        throw Throwables.propagate(e);
      }
    }

    return credential.createScoped(scopes);
  }

  private synchronized void loadCredential() throws IOException {
    if (credential != null) {
      return;
    }

    credential = GoogleCredential.getApplicationDefault();
  }
}
