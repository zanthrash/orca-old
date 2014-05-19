package com.netflix.bluespar.orca.bakery.api

import feign.RequestLine

import javax.inject.Named

interface BakeryService {

    @RequestLine("POST /api/v1/{region}/bake")
    BakeStatus createBake(@Named("region") String region)

    @RequestLine("GET /api/v1/{region}/status/{id}")
    BakeStatus lookupStatus(@Named("region") String region, @Named("id") String id)

}