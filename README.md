# Overview

  MapReduce as a Service:
  * Spin up a Hadoop cluster on HPCloud
  * Submit MapReduce jobs to a cluster
  * Store job results in Swift
  * Tear down clusters


# Prerequisites

* for the command line clients:
   $ sudo gem install json && sudo gem install httparty

# Running The Application

Run MRaaS with the following commands:

* To package:

        mvn package

* To setup the h2 database:

        ./bin/setup_db

* To start the service:

        ./bin/server

* A rest client which exercises the service:

        ./bin/client --help

# Setting up eclipse

  * mvn eclipse:eclipse


# Submitting Code

All merges to master go through Gerritt (https://review.stackforge.org/):

    git review -v


# Gotchas

  Public IPs are re-used, which can cause ssh to complain.
  To prevent this, add the following to ~/.ssh/config:

  Host 15.185.*.*
    UserKnownHostsFile /dev/null
    StrictHostKeyChecking no
