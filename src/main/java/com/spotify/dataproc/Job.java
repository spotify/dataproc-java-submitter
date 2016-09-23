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

import com.google.common.base.Preconditions;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Representation of a Hadoop job to be executed. Includes all details and parameters required
 * for executing the given job.
 */
public class Job {

  private Optional<String> jarPath;
  private Optional<String> mainClass;
  private List<String> args;
  private String[] shippedFiles;
  private String[] shippedJars;
  private String jobId;
  private Map<String, String> properties;

  /**
   * Create a {@link Job} instance for executing the specified main class of the given jar.
   * @param jarPath optional path to the jar file including the job to be executed
   * @param mainClass optional name of the main class to be executed
   * @param args arguments to be passed to the job
   * @param shippedFiles paths to files to be made available to the job
   * @param shippedJars paths to jars to be made available to the job
   * @param jobId a job identifier
   * @param properties properties overriding the Hadoop configuration settings
   */
  public Job(Optional<String> jarPath, Optional<String> mainClass, List<String> args, String[]
      shippedFiles, String[] shippedJars, String jobId, Map<String, String> properties) {
    this.jarPath = jarPath;
    this.mainClass = mainClass;
    this.args = args;
    this.shippedFiles = shippedFiles;
    this.shippedJars = shippedJars;
    this.jobId = jobId;
    this.properties = properties;
  }

  /**
   * Get the optional path of the job jar.
   * @return the path of the jar
   */
  public Optional<String> getJarPath() {
    return jarPath;
  }

  /**
   * Get the optional main class name
   * @return the name of the main class as an optional
   */
  public Optional<String> getMainClass() {
    return mainClass;
  }

  /**
   * Get the list of arguments to be passed to the job
   * @return the list of arguments
   */
  public List<String> getArgs() {
    return args;
  }

  /**
   * Get the list of job arguments as a space separated string
   * @return a string representing the list of arguments
   */
  public String getArgsString() {
    return args.stream().collect(Collectors.joining(" "));
  }

  /**
   * Paths for files to be predistributed and made available to containers
   * @return array of file paths
   */
  public String[] getShippedFiles() {
    return shippedFiles;
  }

  /**
   * Paths for jar files to be predistributed and made available to containers
   * @return array of jar file paths
   */
  public String[] getShippedJars() {
    return shippedJars;
  }

  /**
   * Get the job identifier
   * @return the job identifier
   */
  public String getJobId() {
    return jobId;
  }

  /**
   * Properties overriding the Hadoop configuration settings
   * @return the properties
   */
  public Map<String, String> getProperties() {
    return properties;
  }

  @Override
  public String toString() {
    return "Job{" +
        "jarPath='" + jarPath + '\'' +
        ", mainClass=" + mainClass +
        ", args=" + args +
        ", shippedFiles=" + Arrays.toString(shippedFiles) +
        ", shippedJars=" + Arrays.toString(shippedJars) +
        ", jobId='" + jobId + '\'' +
        ", properties=" + properties +
        '}';
  }

  /**
   * Get a {@link Job} builder
   * @return the builder
   */
  public static Builder builder() {
    return new Builder();
  }

  /**
   * A builder for {@link Job}s.
   */
  public static class Builder {

    private Optional<String> jarPath = Optional.empty();
    private Optional<String> mainClass = Optional.empty();
    private List<String> args = Collections.emptyList();
    private String[] shippedFiles;
    private String[] shippedJars;
    private String jobId;
    private Map<String, String> properties = new LinkedHashMap<>();

    /**
     * Set the path of the job jar.
     * @return the builder instance
     */
    public Builder setJarPath(String jarPath) {
      Preconditions.checkArgument(!mainClass.isPresent(), "one-of [jarPath, mainClass]");
      this.jarPath = Optional.of(jarPath);
      return this;
    }

    /**
     * Set the optional main class name
     * @return the builder instance
     */
    public Builder setMainClass(String mainClass) {
      Preconditions.checkArgument(!jarPath.isPresent(), "one-of [jarPath, mainClass]");
      this.mainClass = Optional.of(mainClass);
      return this;
    }

    /**
     * Set the list of arguments to be passed to the job
     * @return the builder instance
     */
    public Builder setArgs(List<String> args) {
      this.args = args;
      return this;
    }

    /**
     * Set the paths for files to be predistributed and made available to containers
     * @return the builder instance
     */
    public Builder setShippedFiles(String[] shippedFiles) {
      this.shippedFiles = shippedFiles;
      return this;
    }

    /**
     * Set the paths for jar files to be predistributed and made available to containers
     * @return the builder instance
     */
    public Builder setShippedJars(String[] shippedJars) {
      this.shippedJars = shippedJars;
      return this;
    }

    /**
     * Set a job identifier
     * @param jobId the job identifier
     * @return the builder instance
     */
    public Builder setJobId(String jobId) {
      this.jobId = jobId;
      return this;
    }

    /**
     * Set property overriding the Hadoop configuration settings
     * @return the builder instance
     */
    public Builder setProperty(String key, String value) {
      this.properties.put(key, value);
      return this;
    }

    /**
     * Set properties overriding the Hadoop configuration settings
     * @return the builder instance
     */
    public Builder setProperties(Map<String, String> properties) {
      this.properties.putAll(properties);
      return this;
    }


    /**
     * Build the {@link Job} instance with the given settings.
     * @return the {@link Job} instance
     */
    public Job createJob() {
      return new Job(jarPath, mainClass, args, shippedFiles, shippedJars, jobId, properties);
    }
  }
}
