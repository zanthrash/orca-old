package com.netflix.spinnaker.orca.cassandra

interface IdentifierGenerator {

    long next()
}