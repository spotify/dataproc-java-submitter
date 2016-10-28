# dataproc-java-submitter

[![CircleCI](https://circleci.com/gh/spotify/dataproc-java-submitter/tree/master.svg?style=shield)](https://circleci.com/gh/spotify/dataproc-java-submitter)
[![Maven Central](https://img.shields.io/maven-central/v/com.spotify/dataproc-java-submitter.svg)](https://search.maven.org/#search%7Cga%7C1%7Cg%3A%22com.spotify%22%20dataproc-java-submitter)
[![License](https://img.shields.io/github/license/spotify/dataproc-java-submitter.svg)](LICENSE.txt)

A small java library for submitting Hadoop jobs to [Google Cloud Dataproc] from Java.

## Why?

In many real world usages of Hadoop, the jobs are usually parameterized to some degree.
Parameters can be anything from job configuration to input paths. It is common to resolve
these parameter arguments in some workflow tool that eventually puts the arguments on a
command line that is passed to the Hadoop job. On the job side, these arguments have to be
parsed using various tools that are more or less standard.

However if the argument resolution environment is in a JVM, dropping down to a shell and
invoking a command line can be pretty complicated and roundabout. It is also very limiting in
terms of what can be passed to the job. It is not uncommon to take more structured data and
store in some seralized format, stage the files, and have custom logic in the job to
deserialize it.

This library aims to more seamlessly bridge between a local JVM instance and the Hadoop
application entrypoint.

## Usage

### Maven dependency

```xml
<dependency>
  <groupId>com.spotify</groupId>
  <artifactId>dataproc-java-submitter</artifactId>
  <version><!-- see version in maven badge above --></version>
</dependency>
```

### Example usage

```java
String project = "gcp-project-id";
String cluster = "dataproc-cluster-id";

DataprocHadoopRunner hadoopRunner = DataprocHadoopRunner.builder(project, cluster).build();
DataprocLambdaRunner lambdaRunner = DataprocLambdaRunner.forDataproc(hadoopRunner);

// Use any structured type that is Java Serializable
MyStructuredJobArguments arguments = resolveArgumentsInLocalJvm();

lambdaRunner.runOnCluster(() -> {

  // This lambda, including its closure will run on the Dataproc cluster
  System.out.println("Running on the cluster, with " + arguments.inputPaths());

  return 42; // rfc: is it worth supporting a return value from the job?
});
```

The `DataprocLambdaRunner` will take care of configuring the Dataproc job so that it can
run your lambda function. It will scan your local classpath and ensure that the loaded 
jars are staged and configured for the Dataproc job. It will also take care of serializing,
staging and deserializing the lambda closure that is to be invoked on the cluster.

_Note that anything referenced from the lambda has to implement `java.io.Serializable`_

### Low level usage

This library can also be used to configure the Dataproc job directly.

```java
String project = "gcp-project-id";
String cluster = "dataproc-cluster-id";

DataprocHadoopRunner hadoopRunner = DataprocHadoopRunner.builder(project, cluster).build();

Job job = Job.builder()
    .setMainClass(...)
    .setArgs(...)
    .setProperties(...)
    .setShippedJars(...)
    .setShippedFiles(...)
    .createJob();


hadoopRunner.submit(job);
```

[Google Cloud Dataproc]: https://cloud.google.com/dataproc/
