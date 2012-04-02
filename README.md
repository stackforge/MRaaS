# Overview

  MapReduce as a Service


# Prerequisites

* Install openstack-sdk (this needs to be put up on a mvn server somewhere)

    git clone git@github.com:echohead/openstack-java-sdk.git 
    cd openstack-java-sdk 
    mvn install -Dmaven.test.skip=true

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
