# Overview

  MapReduce as a Service


# Prerequisites

* Install openstack-sdk (this needs to be put up on a mvn server somewhere)

    git clone git@github.com:echohead/openstack-java-sdk.git && cd openstack-java-sdk && mvn install

# Running The Application

Run MRaaS with the following commands:

* To package:

        mvn package

* To setup the h2 database:

        java -jar target/mraas-0.0.1-SNAPSHOT.jar setup dev-config.yml

* To start the service:

        java -jar target/mraas-0.0.1-SNAPSHOT.jar server dev-config.yml

