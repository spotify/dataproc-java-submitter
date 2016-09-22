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
package com.spotify.dataproc.submitter;

import com.google.common.base.Throwables;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Optional;

/**
 * Main entrypoint of Hadoop run for continuations executed with
 * {@link DataprocLambdaRunner#runOnCluster(Fn)}
 */
public final class ContinuationEntryPoint {

  private static final String CONT_FILE = "continuation-";
  private static final String SER = ".ser";

  private ContinuationEntryPoint() {
    // no external instantiation
  }

  public static void main(String[] args) throws IOException, ClassNotFoundException {
    String cwd = System.getProperty("user.dir");

    System.out.println("Continuation main, with args " + Arrays.asList(args));

    final Optional<Path> stateFileOpt = Files.list(Paths.get(cwd))
        .filter(ContinuationEntryPoint::isStateFile)
        .findFirst();

    if (stateFileOpt.isPresent()) {
      final File stateFile = stateFileOpt.get().toFile();
      final Fn<?> continuation = readContinuation(stateFile);

      // continue
      final Object output = continuation.run();
      System.out.println("output = " + output);
    }
  }

  static Path serializeContinuation(Fn<?> continuation) {
    try {
      final Path stateFilePath = Files.createTempFile(CONT_FILE, SER);
      final File file = stateFilePath.toFile();
      try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file))) {
        oos.writeObject(continuation);
      }
      return stateFilePath;
    } catch (IOException e) {
      e.printStackTrace();
      throw Throwables.propagate(e);
    }
  }

  private static Fn<?> readContinuation(File stateFile)
      throws IOException, ClassNotFoundException {
    try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(stateFile))) {
      return (Fn<?>) ois.readObject();
    }
  }

  private static boolean isStateFile(Path path) {
    final String fileName = path.getFileName().toString();
    return fileName.startsWith(CONT_FILE) && fileName.endsWith(SER);
  }
}
