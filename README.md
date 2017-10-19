# Distributed File System

The Distributed File System project is an eventually consistent distributed file system, which makes use of different techniques such as Gossiping, Consistent Hashing, Hinted Handoff, Vector Clocks and Anti-Entropy.

<!-- TOC depthFrom:2 depthTo:6 withLinks:1 updateOnSave:1 orderedList:0 -->

- [Introduction](#introduction)
- [Building project](#building-project)
	- [Maven](#maven)
	- [Gradle](#gradle)
- [User guide](#user-guide)
	- [Distributed Nodes](#distributed-nodes)
	- [Client Node](#client-node)
	- [Local Environment](#local-environment)
	- [Pseudo-Distributed Environment](#pseudo-distributed-environment)
- [References](#references)
	- [Java libraries](#java-libraries)
	- [Build tools](#build-tools)

<!-- /TOC -->

## Introduction

The file system is implemented in a hierarchical manner as a key-value map of type (string, file) with the following operations:

- **get**(key) to retrieve a file from the system;
- **put**(key) to store a file in the system;
- **delete**(key) to delete a file in the system. If the target is a folder all its content will be recursively deleted;
- **getAll**() to retrieve all the files stored in a random node.

## Building project

All the project can be built using either the **gradle** or **maven** tool.

### Maven

The whole project can be automatically built using the script inside the **maven scripts** folder, just tiping:

```bash
sh buildAll.sh (Linux and MacOS systems)
start buildAll.bat (Windows systems)
```

Alternatively you can manually built the project, following the commands present in the script.

### Gradle

You don't need to download **gradle**, because you can use the gradle wrapper that provides for you all the necessary tools. It automatically starts when you launch one of the subsequent commands. You have only to set the **JAVA_HOME** environment variable.

The Client can be built with:
```bash
./gradlew client:build
```
which recursively build the Server and Gossip modules.

The StorageNode and LoadBalancer nodes can be built using the following command:
```bash
./gradlew server:build
```
which recursively build the Gossip module.

The Gossip module can be built using the following command:
```bash
./gradlew gossip:build
```

## User Guide

If you don't provide a resource or database location, from both input than from file, the default ones will be used. They are, respectively, **Resources/** and **Database/**.
The setting files, when not specified, are: for client **Settings/ClientSettings.json**, for LoadBalancer and StorageNode **Settings/NodeSettings.json**.

The structure of the **ClientSettings.json** file is the following:

- `network_interface` it can contains `type:`, `IPversion:` and `address:`
- `members` list of nodes in the form `host:, port:, nodeType:`

The structure of the **NodeSettings.json** file is the following:

- `network_interface` it can contains `type:`, `IPversion:` and `address:`
- `Port` Port used to start the gossiping protocol; it can be 0
- `vNodes` number of virtual nodes; it can be 0
- `ResourcesLocation` resources location
- `DatabaseLocation` database location
- `members` list of nodes in the form `host:, port:, nodeType:`

All the elements specified above are optional. Any missing parameter will be setted automatically by the application.

The files for logs and gossiping cannot be changed, and their path is, respectively, **Settings/log4j.properties** and **Settings/GossipSettings.json**.
All the setting files must be placed in the same folder as the JAR file.

**Requirements**:

- Java8
- *Vagrant and VirtualBox (optional, only for a Pseudo-Distributed Environment)*

### Distributed Nodes

The StorageNode and LoadBalancer nodes can be launched with:
```bash
java -jar NodeLauncher-<version>.jar [parameters]
```

The only mandatory parameter is:

- `-t <nodeType>` to start a LoadBalancer (type = 0) or a StorageNode (type = 1) node

Optional parameters:

- `-f [--file] <path>` file used to configure the initial parameters. If present this must be the only specified option
- `-p [--port] <value>` port used to launch the gossiping protocol; the next 5 ports are then used
- `-a [--addr] <value>` set the ip address of the node
- `-n [--node] <arg>` add a new node, where arg is in the format `hostname:port:nodeType`
- `-v [--vnodes] <value>` set the number of virtual nodes. This option is valid only for StorageNode nodes
- `-r [--rloc] <path>` set the location of the resources. This option is valid only for StorageNode nodes
- `-d [--dloc] <path>` set the location of the database. This option is valid only for StorageNode nodes
- `-h [--help]` show the help informations

### Client Node

The Client can be launched with:
```bash
java -jar Client-<version>.jar [parameters]
```
Optional parameters:

- `-f [--file] <path>` file used to configure the initial parameters. If present this must be the only specified option
- `-p [--port] <value>` port used to receive the remote connection
- `-a [--addr] <value>` set the ip address of the node
- `-n [--node] <arg>` add a new node, where arg is in the format `hostname:port:nodeType`
- `-r [--rloc] <path>` set the location of the resources
- `-d [--dloc] <path>` set the location of the database
- `-locale` start the system in the [local environment](#local-environment)
- `-h [--help]` show the help informations

The operations provided by the client, in addition to the **get**, **put and delete** mentioned before, are:

- **list** print on screen a list of all the files present in the client’s database;
- **enableLB/disableLB** enable/disable the utilization of the remote load balancer nodes;
- **enableSync/disableSync** enable/disable the utilization of the syncronizer thread;
- **help** to show the helper;
- **exit** to close the service.

The disableLB mode is maintained until the client owns at least one storage node, otherwise the system automatically goes back to using again load balancer nodes.

### Local Environment

If you want to test the system in a multi-threaded environment using your machine, you can do that launching the Client node with the `-locale` option.
If the list of input nodes is empty a fixed number of distributed nodes is used, namely 2 LoadBalancers and 4 StorageNodes.

### Pseudo-Distributed Environment

Using the **Vagrantfile** file provided in the distribution you can run the system in a pseudo-distributed environment. Inside it are defined 6 virtual machines that can be used both for a remote node (LoadBalancer or StorageNode) that for a Client.
My suggestion is to assign the VMs in this way: at least 1 for Client and LoadBalancer and at least 3 for the StorageNodes just to be sure that the quorum can be reached.

## References
### Java libraries

- [MapDB](http://www.mapdb.org/), to implement the storage system.
- [JUnit](http://junit.org/), to test the project.
- [Log4j](http://logging.apache.org/log4j/2.x/), to manage the log system of the project.
- [commons-cli](https://commons.apache.org/proper/commons-cli/), to parse the command options.
- [commons-collections](https://commons.apache.org/proper/commons-collections/), contains lot of efficient data structures.
- [jline](http://jline.sourceforge.net/), to read the user input.
- [guava](https://github.com/google/guava), to compute the id of the objects using an hash function.
- [json](https://github.com/stleary/JSON-java), to read and write JSON files.
- [mr-udp](http://lac-rio.com/mr-udp/), to implement the Reliable UDP protocol.
- [gossiping](https://github.com/tonellotto/Distributed-Enabling-Platforms/tree/master/gossiping), customized version of the gossiping protocol.
- [versioning](https://github.com/tonellotto/Distributed-Enabling-Platforms/tree/master/versioning), cloned version of the Voldemort project.

### Build tools

- [Gradle](https://gradle.org/), to build all the project and manage the dependencies.
- [JitPack](https://jitpack.io), to build Java library from github.
