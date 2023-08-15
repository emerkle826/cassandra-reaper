/*
 * Copyright 2018-2018 The Last Pickle Ltd
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.cassandrareaper.management;

import java.util.Map;

public final class FailureDetectorProxy {

  private final ICassandraManagementProxy proxy;

  private FailureDetectorProxy(ICassandraManagementProxy proxy) {
    this.proxy = proxy;
  }

  public static FailureDetectorProxy create(ICassandraManagementProxy proxy) {

    return new FailureDetectorProxy((ICassandraManagementProxy) proxy);
  }


  public String getAllEndpointsState() {
    return proxy.getAllEndpointStates();
  }

  public Map<String, String> getSimpleStates() {
    return proxy.getSimpleStates();
  }
}