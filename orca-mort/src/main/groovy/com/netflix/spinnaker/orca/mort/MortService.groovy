/*
 * Copyright 2014 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.netflix.spinnaker.orca.mort

import retrofit.client.Response
import retrofit.http.Body
import retrofit.http.GET
import retrofit.http.POST
import retrofit.http.Path
import retrofit.http.Query

interface MortService {
  @GET("/securityGroups/{account}/{type}/{securityGroupName}")
  Response getSecurityGroup(
    @Path("account") String account,
    @Path("type") String type, @Path("securityGroupName") String securityGroupName, @Query("region") String region)

  @GET("/search")
  Response getSearchResults(@Query("q") String searchTerm,
                            @Query("type") String type)

  @POST("/cache/{type}")
  Response forceCacheUpdate(@Path("type") String type, @Body Map<String, ? extends Object> data)
}
