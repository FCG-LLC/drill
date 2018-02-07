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

import org.apache.drill.common.logical.StoragePluginConfigBase;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;

@JsonTypeName(KuduStoragePluginConfig.NAME)
public class KuduStoragePluginConfig extends StoragePluginConfigBase {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(KuduStoragePluginConfig.class);

  public static final String NAME = "kudu";

  private final String masterAddresses;
  private final long operationTimeoutMs;
  private final int optimizerMaxNonPrimaryKeyAlternatives;
  private final boolean allUnsignedINT8;
  private final boolean allUnsignedINT16;
  private final boolean allUnsignedINT32;

  @JsonCreator
  public KuduStoragePluginConfig(
          @JsonProperty("masterAddresses") String masterAddresses,
          @JsonProperty("operationTimeoutMs") long operationTimoutMs,
          @JsonProperty("optimizerMaxNonPrimaryKeyAlternatives") int optimizerMaxNonPrimaryKeyAlternatives,
          @JsonProperty("allUnsignedINT8") boolean allUnsignedINT8,
          @JsonProperty("allUnsignedINT16") boolean allUnsignedINT16,
          @JsonProperty("allUnsignedINT32") boolean allUnsignedINT32) {
    this.masterAddresses = masterAddresses;
    this.operationTimeoutMs = operationTimoutMs;
    this.optimizerMaxNonPrimaryKeyAlternatives = optimizerMaxNonPrimaryKeyAlternatives;
    this.allUnsignedINT8 = allUnsignedINT8;
    this.allUnsignedINT16 = allUnsignedINT16;
    this.allUnsignedINT32 = allUnsignedINT32;
  }

  public String getMasterAddresses() {
    return masterAddresses;
  }

  public long getOperationTimeoutMs() { return operationTimeoutMs; }

  public int getOptimizerMaxNonPrimaryKeyAlternatives() { return optimizerMaxNonPrimaryKeyAlternatives; }

  public boolean isAllUnsignedINT8() { return allUnsignedINT8; }

  public boolean isAllUnsignedINT16() { return allUnsignedINT16; }

  public boolean isAllUnsignedINT32() { return allUnsignedINT32; }

  @Override
  public boolean equals(Object o) {
    if (this == o) { return true; }
    if (o == null || getClass() != o.getClass()) { return false; }

    KuduStoragePluginConfig that = (KuduStoragePluginConfig) o;

    if (operationTimeoutMs != that.operationTimeoutMs) { return false; }
    if (optimizerMaxNonPrimaryKeyAlternatives != that.optimizerMaxNonPrimaryKeyAlternatives) { return false; }
    if (allUnsignedINT8 != that.allUnsignedINT8) { return false; }
    if (allUnsignedINT16 != that.allUnsignedINT16) { return false; }
    if (allUnsignedINT32 != that.allUnsignedINT32) { return false; }

    return masterAddresses != null ? masterAddresses.equals(that.masterAddresses) : that.masterAddresses == null;
  }

  @Override
  public int hashCode() {
    int result = masterAddresses != null ? masterAddresses.hashCode() : 0;
    result = 31 * result + (int) (operationTimeoutMs ^ (operationTimeoutMs >>> 32));
    result = 31 * result + optimizerMaxNonPrimaryKeyAlternatives;
    result = 31 * result + (allUnsignedINT8 ? 1 : 0);
    result = 31 * result + (allUnsignedINT16 ? 1 : 0);
    result = 31 * result + (allUnsignedINT32 ? 1 : 0);
    return result;
  }
}
