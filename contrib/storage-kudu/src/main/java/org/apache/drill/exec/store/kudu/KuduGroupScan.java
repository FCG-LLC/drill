/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.store.kudu;

import java.io.*;
import java.util.*;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.physical.EndpointAffinity;
import org.apache.drill.exec.physical.base.AbstractGroupScan;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.base.PhysicalOperator;
import org.apache.drill.exec.physical.base.ScanStats;
import org.apache.drill.exec.physical.base.ScanStats.GroupScanProperty;
import org.apache.drill.exec.proto.CoordinationProtos.DrillbitEndpoint;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.StoragePluginRegistry;
import org.apache.drill.exec.store.kudu.KuduSubScan.KuduSubScanSpec;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.drill.exec.store.schedule.AffinityCreator;
import org.apache.drill.exec.store.schedule.AssignmentCreator;
import org.apache.drill.exec.store.schedule.CompleteWork;
import org.apache.drill.exec.store.schedule.EndpointByteMap;
import org.apache.drill.exec.store.schedule.EndpointByteMapImpl;
import org.apache.kudu.Common;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.apache.kudu.client.LocatedTablet.Replica;

import static org.apache.drill.exec.store.AbstractRecordReader.STAR_COLUMN;

@JsonTypeName("kudu-scan")
public class KuduGroupScan extends AbstractGroupScan {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(KuduGroupScan.class);
  private static final long DEFAULT_TABLET_SIZE = 1000;

  private KuduStoragePluginConfig storagePluginConfig;
  private List<SchemaPath> columns;
  private KuduScanSpec kuduScanSpec;
  private KuduStoragePlugin storagePlugin;
  private boolean filterPushedDown = false;
  private List<KuduWork> kuduWorkList = Lists.newArrayList();
  private ListMultimap<Integer,KuduWork> assignments;
  private List<EndpointAffinity> affinities;

  private KuduClient client;
  private KuduTable table;
  private Schema tableSchema;

  @JsonCreator
  public KuduGroupScan(@JsonProperty("kuduScanSpec") KuduScanSpec kuduScanSpec,
                        @JsonProperty("storage") KuduStoragePluginConfig storagePluginConfig,
                        @JsonProperty("columns") List<SchemaPath> columns,
                        @JacksonInject StoragePluginRegistry pluginRegistry) throws IOException, ExecutionSetupException {
    this((KuduStoragePlugin) pluginRegistry.getPlugin(storagePluginConfig), kuduScanSpec, columns);
  }

  public KuduGroupScan(KuduStoragePlugin storagePlugin, KuduScanSpec scanSpec,
      List<SchemaPath> columns) {
    super((String) null);
    this.storagePlugin = storagePlugin;
    this.storagePluginConfig = storagePlugin.getConfig();
    this.kuduScanSpec = scanSpec;
    this.columns = columns == null || columns.size() == 0? ALL_COLUMNS : columns;

    init();
  }

  private List<KuduScanToken> initScanTokens() throws KuduException {
    ArrayList<KuduScanToken> allScanTokens = new ArrayList<>();

    List<List<KuduPredicate>> predicatePermutationSets = new ArrayList<>();
    predicatePermutationSets.add(new ArrayList<KuduPredicate>());

    // Idea is following, if we have a scan spec:
    //     predicates: x > 0
    //     orSet:
    //        predicates: y = 10, y = 20
    // We will want to convert it to two scans (permutate it):
    //     x > 0 and y = 10
    //     x > = and y = 20
    //
    // With all this scan tokens Drill should handle the rest
    //
    predicatePermutationSets = permutateScanSpec(predicatePermutationSets, kuduScanSpec);

    for (List<KuduPredicate> predicateSet : predicatePermutationSets) {
      KuduScanToken.KuduScanTokenBuilder scanTokenBuilder = client.newScanTokenBuilder(table);

      if (!AbstractRecordReader.isStarQuery(columns)) {
        List<String> colNames = Lists.newArrayList();
        for (SchemaPath p : this.getColumns()) {
          colNames.add(p.getAsUnescapedPath());
        }

        // We must set projected columns in order, otherwise nasty things
        // related to primary (composite) key columns might happen
        Collections.sort(colNames, new Comparator<String>() {
          @Override
          public int compare(String o1, String o2) {
            return table.getSchema().getColumnIndex(o1) - table.getSchema().getColumnIndex(o2);
          }
        });

        scanTokenBuilder.setProjectedColumnNames(colNames);
      }

      KuduScanSpec pseudoScanSpec = new KuduScanSpec(getTableName(), predicateSet);
      System.out.println(pseudoScanSpec.toString());

      for (KuduPredicate pred : predicateSet) {
        scanTokenBuilder.addPredicate(pred);
      }

      allScanTokens.addAll(scanTokenBuilder.build());
    }

    return allScanTokens;
  }

  private List<List<KuduPredicate>> permutateScanSpec(List<List<KuduPredicate>> inputPermutationSets, KuduScanSpec scanSpec) {
    List<List<KuduPredicate>> newLists = new ArrayList<>();

    if (scanSpec.isPushOr()) {
      // We are going to expand the tree with new options. Wunderbar!

      for (List<KuduPredicate> list : inputPermutationSets) {
        for (KuduPredicate pred : scanSpec.getPredicates()) {
          List<KuduPredicate> newList = new ArrayList<>(list);
          newList.add(pred);
          newLists.add(newList);
        }
      }

      for (KuduScanSpec subSet : scanSpec.getSubSets()) {
        newLists.addAll(permutateScanSpec(inputPermutationSets, subSet));
      }
    } else {
      // This is just AND, so add constraints to the current set
      for (List<KuduPredicate> list : inputPermutationSets) {
        List<KuduPredicate> newList = new ArrayList<>(list);
        for (KuduPredicate pred : scanSpec.getPredicates()) {
          newList.add(pred);
        }
        newLists.add(newList);
      }

      for (KuduScanSpec subSet : scanSpec.getSubSets()) {
        // This is probably wrong
        newLists.addAll(permutateScanSpec(inputPermutationSets, subSet));
      }
    }

    return newLists;
  }

  private void initFields() {
    this.client = storagePlugin.getClient();
    try {
      this.table = client.openTable(kuduScanSpec.getTableName());
      this.tableSchema = this.table.getSchema();
    } catch (KuduException ke) {
      throw new RuntimeException(ke);
    }
  }

  private void init() {
    initFields();

    Collection<DrillbitEndpoint> endpoints = storagePlugin.getContext().getBits();
    Map<String,DrillbitEndpoint> endpointMap = Maps.newHashMap();
    for (DrillbitEndpoint endpoint : endpoints) {
      endpointMap.put(endpoint.getAddress(), endpoint);
    }

    try {
      final List<KuduScanToken> scanTokens = initScanTokens();

      for (KuduScanToken scanToken : scanTokens) {
        KuduWork work = new KuduWork(scanToken.serialize());

        for (Replica replica : scanToken.getTablet().getReplicas()) {
          String host = replica.getRpcHost();
          DrillbitEndpoint ep = endpointMap.get(host);
          if (ep != null) {
            work.getByteMap().add(ep, DEFAULT_TABLET_SIZE);
          }
        }
        kuduWorkList.add(work);

      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @JsonIgnore
  public Schema getTableSchema() { return this.tableSchema; }



  private static class KuduWork implements CompleteWork {
    private EndpointByteMapImpl byteMap = new EndpointByteMapImpl();
    private byte[] serializedScanToken;

    public KuduWork(byte[] serializedScanToken) {
      this.serializedScanToken = serializedScanToken;
    }

    @Override
    public long getTotalBytes() {
      return DEFAULT_TABLET_SIZE;
    }

    @Override
    public EndpointByteMap getByteMap() {
      return byteMap;
    }

    @Override
    public int compareTo(CompleteWork o) {
      return 0;
    }

    public byte[] getSerializedScanToken() {
      return serializedScanToken;
    }
  }

  /**
   * Private constructor, used for cloning.
   * @param that The KuduGroupScan to clone
   */
  private KuduGroupScan(KuduGroupScan that) {
    super(that);
    this.columns = that.columns;
    this.kuduScanSpec = that.kuduScanSpec;
    this.storagePlugin = that.storagePlugin;
    this.storagePluginConfig = that.storagePluginConfig;
    this.filterPushedDown = that.filterPushedDown;
    this.kuduWorkList = that.kuduWorkList;
    this.assignments = that.assignments;
    this.table = that.table;
    this.tableSchema = that.tableSchema;
  }

  @Override
  public GroupScan clone(List<SchemaPath> columns) {
    KuduGroupScan newScan = new KuduGroupScan(this);
    newScan.columns = columns;
    return newScan;
  }

  @Override
  public List<EndpointAffinity> getOperatorAffinity() {
    if (affinities == null) {
      affinities = AffinityCreator.getAffinityMap(kuduWorkList);
    }
    return affinities;
  }


  @Override
  public int getMaxParallelizationWidth() {
    return kuduWorkList.size();
  }


  /**
   *
   * @param incomingEndpoints
   */
  @Override
  public void applyAssignments(List<DrillbitEndpoint> incomingEndpoints) {
    assignments = AssignmentCreator.getMappings(incomingEndpoints, kuduWorkList);
  }

  @Override
  public KuduSubScan getSpecificScan(int minorFragmentId) {
    List<KuduWork> workList = assignments.get(minorFragmentId);

    List<KuduSubScanSpec> scanSpecList = Lists.newArrayList();

    logger.info(kuduScanSpec.toString());

    for (KuduWork work : workList) {
      scanSpecList.add(new KuduSubScanSpec(getTableName(), work.getSerializedScanToken()));
    }

    return new KuduSubScan(storagePlugin, storagePluginConfig, scanSpecList, this.columns);
  }

  // KuduStoragePlugin plugin, KuduStoragePluginConfig config,
  // List<KuduSubScanSpec> tabletInfoList, List<SchemaPath> columns
  @Override
  public ScanStats getScanStats() {
    // Very naive - the more constraints the better...
    //int constraintsDenominator = kuduScanSpec.getPredicates().size() + 1;
    int constraintsDenominator = kuduScanSpecSize(0, kuduScanSpec) + 1;
    //long recordCount = (100000 / constraintsDenominator) * kuduWorkList.size();
    long recordCount = (100000 / constraintsDenominator);

    return new ScanStats(GroupScanProperty.NO_EXACT_ROW_COUNT, recordCount, 1/((float) constraintsDenominator), recordCount);
  }

  private int kuduScanSpecSize(int size, KuduScanSpec cur) {
    size += cur.getPredicates().size();
    for (KuduScanSpec next : cur.getSubSets()) {
      size += kuduScanSpecSize(0, next);
    }

    return size;
  }

  @Override
  @JsonIgnore
  public PhysicalOperator getNewWithChildren(List<PhysicalOperator> children) {
    Preconditions.checkArgument(children.isEmpty());
    return new KuduGroupScan(this);
  }

  @JsonIgnore
  public KuduStoragePlugin getStoragePlugin() {
    return storagePlugin;
  }

  @JsonIgnore
  public String getTableName() {
    return getKuduScanSpec().getTableName();
  }

  @Override
  public String getDigest() {
    return toString();
  }

  @Override
  public String toString() {
    return "KuduGroupScan [KuduScanSpec="
        + kuduScanSpec + ", columns="
        + columns + "]";
  }

  @JsonProperty("storage")
  public KuduStoragePluginConfig getStorageConfig() {
    return this.storagePluginConfig;
  }

  @JsonProperty
  public List<SchemaPath> getColumns() {
    return columns;
  }

  @JsonProperty
  public KuduScanSpec getKuduScanSpec() {
    return kuduScanSpec;
  }

  @Override
  @JsonIgnore
  public boolean canPushdownProjects(List<SchemaPath> columns) {
    return true;
  }

  @JsonIgnore
  public void setFilterPushedDown(boolean b) {
    this.filterPushedDown = true;
  }

  @JsonIgnore
  public boolean isFilterPushedDown() {
    return filterPushedDown;
  }

  /**
   * Empty constructor, do not use, only for testing.
   */
  @VisibleForTesting
  public KuduGroupScan() {
    super((String)null);
  }

  /**
   * Do not use, only for testing.
   */
  @VisibleForTesting
  public void setKuduScanSpec(KuduScanSpec kuduScanSpec) {
    this.kuduScanSpec = kuduScanSpec;
  }

}
