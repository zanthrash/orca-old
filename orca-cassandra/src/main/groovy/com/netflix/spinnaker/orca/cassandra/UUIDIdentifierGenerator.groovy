package com.netflix.spinnaker.orca.cassandra

import groovy.transform.CompileStatic

@CompileStatic
class UUIDIdentifierGenerator implements IdentifierGenerator {

    @Override
    long next() {
        UUID.randomUUID().mostSignificantBits
    }

}
