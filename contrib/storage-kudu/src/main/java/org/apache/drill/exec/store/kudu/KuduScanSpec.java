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


import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kudu.Schema;
import org.apache.kudu.client.ColumnRangePredicate;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanToken;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class KuduScanSpec {

  private final String tableName;
  private List<KuduPredicate> predicates = new ArrayList<>();
  private List<KuduScanSpec> subSets = new ArrayList<>();

  private boolean pushOr = false;

  @JsonCreator
  public KuduScanSpec(@JsonProperty("tableName") String tableName) {
    this.tableName = tableName;
  }

  public KuduScanSpec(@JsonProperty("tableName") String tableName, @JsonProperty("predicates") KuduPredicate pred) {
    this.tableName = tableName;
    this.predicates.add(pred);
  }

  public KuduScanSpec(@JsonProperty("tableName") String tableName, @JsonProperty("predicates") Collection<KuduPredicate> preds) {
    this.tableName = tableName;
    this.predicates.addAll(preds);
  }

  public String getTableName() {
    return tableName;
  }

  public void setPushOr(boolean pushOr) {
    this.pushOr = pushOr;
  }

  public boolean isPushOr() {
    return pushOr;
  }

  public void addSubSet(KuduScanSpec orSet) {
    this.subSets.add(orSet);
  }

  public List<KuduScanSpec> getSubSets() {
    return this.subSets;
  }

  public List<KuduPredicate> getPredicates() {
    return predicates;
  }

  @Override
  public String toString() {
    return toString(false);
  }

  protected String toString(boolean simplified) {

    StringBuilder sb = new StringBuilder();
    if (!simplified) {
      sb.append("Predicates on table ");
      sb.append(tableName);
      sb.append(": ");
    }

    boolean hasPrev = false;
    for (KuduPredicate pred : predicates) {
      if (hasPrev) {
        if (isPushOr()) {
          sb.append(" OR ");
        } else {
          sb.append(" AND ");
        }
      } else {
        hasPrev = true;
      }

      sb.append(pred.toString());
    }

    for (KuduScanSpec subSet : subSets) {
      if (hasPrev) {
        sb.append(", ");
      }

      sb.append("{");
      sb.append(subSet.toString(true));
      sb.append("}");
    }



    return sb.toString();
  }
}
