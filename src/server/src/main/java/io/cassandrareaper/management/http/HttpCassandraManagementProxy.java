/*
 * Copyright 2020-2020 The Last Pickle Ltd
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

package io.cassandrareaper.management.http;

import io.cassandrareaper.ReaperException;
import io.cassandrareaper.core.Table;
import io.cassandrareaper.management.ICassandraManagementProxy;
import io.cassandrareaper.management.jmx.RepairStatusHandler;
import io.cassandrareaper.service.RingRange;

import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.management.JMException;
import javax.management.Notification;
import javax.validation.constraints.NotNull;

import com.codahale.metrics.MetricRegistry;
import com.datastax.mgmtapi.client.api.DefaultApi;
import com.datastax.mgmtapi.client.invoker.ApiClient;
import com.datastax.mgmtapi.client.invoker.ApiException;
import org.apache.cassandra.repair.RepairParallelism;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpCassandraManagementProxy implements ICassandraManagementProxy {

  private static final Logger LOG = LoggerFactory.getLogger(HttpCassandraManagementProxy.class);

  String host;
  MetricRegistry metricRegistry;
  String rootPath;
  InetSocketAddress endpoint;
  DefaultApi apiClient;

  public HttpCassandraManagementProxy(MetricRegistry metricRegistry,
                                      String rootPath,
                                      InetSocketAddress endpoint
  ) {
    this.metricRegistry = metricRegistry;
    this.rootPath = rootPath;
    this.endpoint = endpoint;
    // configure the Management API client
    ApiClient api = new ApiClient()
        // with the approrpiate base URL (i.e. "http://localhost:8080")
        .setBasePath(endpoint.getHostString())
        // probably want the default 10 second connection timeout
        //.setConnectTimeout(0)
        // At the very least, the stopNode operation can take longer than the default 10 second read timeout
        .setReadTimeout(0)
        // We might have write operations that take longer than 10 seconds too
        .setWriteTimeout(0);
    // setup the helper client with the configured api
    this.apiClient = new DefaultApi(api);
  }

  @Override
  public String getHost() {
    return host;
  }

  @Override
  public List<BigInteger> getTokens() {
    return null; // TODO: implement me.
  }

  @Override
  public Map<List<String>, List<String>> getRangeToEndpointMap(String keyspace) throws ReaperException {
    return null; // TODO: implement me.
  }

  @NotNull
  @Override
  public String getLocalEndpoint() throws ReaperException {
    return null; // TODO: implement me.
  }

  @NotNull
  @Override
  public Map<String, String> getEndpointToHostId() {
    return null; // TODO: implement me.
  }

  @Override
  public String getPartitioner() {
    return null; // TODO: implement me.
  }

  @Override
  public String getClusterName() {
    return null; // TODO: implement me.
  }

  @Override
  public List<String> getKeyspaces() {
    try {
      return apiClient.listKeyspaces(null);
    } catch (ApiException ae) {
      // API call returned some 5xx response
      // ApiException has the status code and response body as attributes we can pull.
      // ApiException also encodes those bits into the Exception message.
      // TODO: Better 5xx handling
      LOG.warn("Request not successful", ae);
      // return an empty list
      return List.of();
    }
  }

  @Override
  public Set<Table> getTablesForKeyspace(String keyspace) throws ReaperException {
    Set<Table> tables = new HashSet<Table>();
    try {
      for (String tableName : apiClient.listTables(keyspace)) {
        // TODO: Need to fetch table compaction strategy
        Table table = Table.builder().withName(tableName).build();
        tables.add(table);
      }
    } catch (ApiException ae) {
      // API call returned some 5xx response
      // ApiException has the status code and response body as attributes we can pull.
      // ApiException also encodes those bits into the Exception message.
      // TODO: Better 5xx handling
      LOG.warn("Request not successful", ae);
    }
    return tables;
  }

  @Override
  public int getPendingCompactions() throws JMException {
    return 1; // TODO: implement me.
  }

  @Override
  public boolean isRepairRunning() throws JMException {
    return true; // TODO: implement me.
  }


  @Override
  public List<String> getRunningRepairMetricsPost22() {
    return null; // TODO: implement me.
  }

  @Override
  public void cancelAllRepairs() {
    // TODO: implement me.
  }

  @Override
  public Map<String, List<String>> listTablesByKeyspace() {
    return null; // TODO: implement me.
  }

  @Override
  public String getCassandraVersion() {
    return null; // TODO: implement me.
  }

  @Override
  public int triggerRepair(
      BigInteger beginToken,
      BigInteger endToken,
      String keyspace,
      RepairParallelism repairParallelism,
      Collection<String> columnFamilies,
      boolean fullRepair,
      Collection<String> datacenters,
      RepairStatusHandler repairStatusHandler,
      List<RingRange> associatedTokens,
      int repairThreadCount)
      throws ReaperException {
    return 1; //TODO: implement me

  }

  @Override
  public void handleNotification(final Notification notification, Object handback) {
    // TODO: implement me.
  }

  @Override
  public boolean isConnectionAlive() {
    return true; // TODO: implement me.
  }

  @Override
  public void removeRepairStatusHandler(int repairNo) {
    // TODO: implement me.
  }

  @Override
  public void close() {
    // TODO: implement me.
  }

  @Override
  public List<String> getLiveNodes() throws ReaperException {
    return null; // TODO: implement me.
  }

}