package org.apache.drill.exec.store.cslogs;

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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.exec.ops.OptimizerRulesContext;
import org.apache.drill.exec.server.DrillbitContext;
import org.apache.drill.exec.store.AbstractStoragePlugin;
import org.apache.drill.exec.store.SchemaConfig;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;
import org.apache.kudu.client.KuduClient;

import java.io.IOException;
import java.util.Set;


public class CSLogsStoragePlugin extends AbstractStoragePlugin {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CSLogsStoragePlugin.class);

    public static final String LOG_TABLE_NAME = "logs";

    private final DrillbitContext context;
    private final CSLogsStoragePluginConfig engineConfig;
    private CSLogsParamRegistry paramRegistry;
    private CSLogsSchemaFactory schemaFactory;

    @SuppressWarnings("unused")
    private final String name;
    private final KuduClient client;

    public CSLogsStoragePlugin(CSLogsStoragePluginConfig configuration, DrillbitContext context, String name)
            throws IOException {
        this.context = context;
        this.engineConfig = configuration;
        this.name = name;
        this.schemaFactory = new CSLogsSchemaFactory(this, name);
        this.client = new KuduClient.KuduClientBuilder(configuration.getMasterAddresses())
                .defaultAdminOperationTimeoutMs(configuration.getOperationTimeoutMs())
                .defaultOperationTimeoutMs(configuration.getOperationTimeoutMs())
                .defaultSocketReadTimeoutMs(configuration.getOperationTimeoutMs()/2)
                .build();

        // Eventually, this will be dynamic
        this.paramRegistry = new CSLogsParamRegistry();
    }

    @Override
    public void start() throws IOException {

    }

    public KuduClient getClient() {
        return client;
    }

    @Override
    public void close() throws Exception {
        client.close();
    }

    public DrillbitContext getContext() {
        return this.context;
    }

    @Override
    public boolean supportsRead() {
        return true;
    }

    @Override
    public CSLogsGroupScan getPhysicalScan(String userName, JSONOptions selection) throws IOException {
        CSLogsScanSpec scanSpec = selection.getListWith(new ObjectMapper(), new TypeReference<CSLogsScanSpec>() {});
        return new CSLogsGroupScan(this, scanSpec, null);
    }

    @Override
    public boolean supportsWrite() {
        return true;
    }

    @Override
    public void registerSchemas(SchemaConfig schemaConfig, SchemaPlus parent) throws IOException {
        schemaFactory.registerSchemas(schemaConfig, parent);
    }

    @Override
    public CSLogsStoragePluginConfig getConfig() {
        return engineConfig;
    }

    @Override
    public Set<StoragePluginOptimizerRule> getPhysicalOptimizerRules(OptimizerRulesContext optimizerRulesContext) {
        return ImmutableSet.of(CSLogsPushFilterIntoScan.FILTER_ON_SCAN, CSLogsPushFilterIntoScan.FILTER_ON_PROJECT);
    }
}