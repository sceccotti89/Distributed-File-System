# Distributed File System

The Distributed File System project is an eventually consistent distributed file system, which makes use of different techniques, such as Gossiping, Consistent Hashing, Vector Clocks and Anti-Entropy.

<!-- TOC depthFrom:2 depthTo:6 withLinks:1 updateOnSave:1 orderedList:0 -->

- [Introduction](#introduction)
- [User Guide](#user-guide)
	- [Distributed Nodes](#distributed-nodes)
	- [Client Node](#client-node)
	- [Local Environment](#local-environment)
	- [Pseudo-Distributed Environment](#pseudo-distributed-environment)
- [References](#references)
	- [Java libraries:](#java-libraries)
	- [Build tools:](#build-tools)

<!-- /TOC -->

## Introduction



The communication between nodes exploits the Java socket mechanism, thus it can be executed in different ways: on a single machine with threads, on a cluster of servers or in virtual containers using *Docker*.

The file system is implemented as a key value map of type <string, DistributedFile> with the following operations:

- **get**(key) to retrieve a file from the system;
- **put**(key) to store a file in the system;
- **delete**(key) to remove a file in the system;
- **getAll**() to retrieve all the files stored in a random node;


## User Guide

All the project can be built using `gradle` but you don't need to download it, because you can use the gradle wrapper that downloads for you all the necessary tools.

**Requirements**:

- Java8
- *Vagrant and VirtualBox (optional only for a Pseudo-Distributed Environment)*

### Distributed Nodes

The LoadBalancer and StorageNode nodes can be built using the following command:
```bash
./gradlew server:build
```
and runs it with:
```bash
java -jar NodeLauncher-<version>.jar [parameters]
```

The only mandatory parameter is:

- `t <nodeType>` to start a LoadBalancer (type = 0) or a StorageNode (type = 1) node

Optional parameters:

- `p [--port] <value>` port used to listen the incoming requests. This option is valid only for LoadBalancer nodes
- `a [--addr] <value>` set the ip address of the node
- `n [--node] <arg>` add a new node, where arg is in the format `hostname:port:nodeType`
- `r [--rloc] <path>` set the location of the resources. This option is valid only for StorageNode nodes
- `d [--dloc] <path>` set the location of the database. This option is valid only for StorageNode nodes
- `h [--help]` show the help informations

### Client Node

The Client can be built in this way:
```bash
./gradlew client:build
```
and runs it with:
```bash
java -jar Client-<version>.jar [parameters]
```
Optional parameters:

- `p [--port] <value>` port used to receive the remote connection
- `a [--addr] <value>` set the ip address of the node
- `n [--node] <arg>` add a new node, where arg is in the format `hostname:port:nodeType`
- `r [--rloc] <path>` set the location of the resources
- `d [--dloc] <path>` set the location of the database
- `locale` start the system in the local environment
- `h [--help]` show the help informations

### Local Environment

If you want to test the system in a clean environment using your machine, it can be done launching the Client node with the `-locale` option.
If the list of input nodes is empty a fixed number of distributed nodes is used, namely 5 `LoadBalancer`s and 2 `StorageNode`s.

### Pseudo-Distributed Environment

Using the `Vagrantfile` file provided in the distribution you can run the system in a pseudo-distributed environment. In the file are defined 4 VMs for as many `StorageNode`s, 1 for the `LoadBalancer` and another one for the `Client`.

// TODO sistemare le reference
## References
### Java libraries

- [MapDB](http://www.mapdb.org/), to implement the storage.
- [JUnit](http://junit.org/), to test the project.
- [Log4j](http://logging.apache.org/log4j/2.x/), to manage the log system of the project.


### Build tools

- [Gradle](https://gradle.org/), to build all the project and manage the dependencies.
- [JitPack](https://jitpack.io), to build Java library from github.
