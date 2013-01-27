MapReduce as a Service
======================

There are three main parts:

1. Core service layer
2. Hadoop-Swift native driver
3. REST client interface

## Hadoop Swift Native Driver

hadoop-swiftn-driver includes source code base for native driver to stream data from/to Swift object store. It has been tested with Hadoop 1.0.3. To use this driver:

1. Put driver source code into Hadoop source base (tested with Hadoop 1.0.3)
2. Put hpswift.jar into lib directory
3. Build Hadoop
4. Edit conf files accordingly
 