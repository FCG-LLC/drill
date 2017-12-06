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

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduPredicate;

import java.util.ArrayList;
import java.util.List;


public class KuduScanSpecOptimizer {
    private KuduScanSpec input;
    List<String> primaryKeys;
    Schema schema;

    int maxNonPrimaryKeyAlternatives;

    public KuduScanSpecOptimizer(KuduStoragePluginConfig config, KuduScanSpec input, Schema schema) {
        this.input = input;

        primaryKeys = new ArrayList<>();
        for (ColumnSchema columnSchema : schema.getPrimaryKeyColumns()) {
            primaryKeys.add(columnSchema.getName());
        }

        this.maxNonPrimaryKeyAlternatives = config.getOptimizerMaxNonPrimaryKeyAlternatives();
        this.schema = schema;
    }

    /**
     * Flattens the given scan spec structure towards a set of linear KuduPredicate lists (each of the list is
     * a conjuction of predicates; however, the lists are then summed, so they are alternatives).
     *
     * Idea is following, if we have a scan spec:
     *     predicates: x > 0
     *     orSet:
     *        predicates: y = 10, y = 20
     * We will want to convert it to two scans (permutate it):
     *     x > 0 and y = 10
     *     x > = and y = 20
     *
     * With all this scan tokens Drill should handle the rest
     */
    public List<List<KuduPredicate>> optimizeScanSpec(KuduScanSpec scanSpec) {
        List<List<KuduPredicate>> inputPermutationSets = new ArrayList<>();
        inputPermutationSets.add(new ArrayList<KuduPredicate>());

        List<List<KuduPredicate>> basePermutationSet = permutateScanSpec(inputPermutationSets, scanSpec);

        NonPrimaryKeyPermutationPruner pruner = new NonPrimaryKeyPermutationPruner(this, basePermutationSet);
        List<List<KuduPredicate>> prunedSet = pruner.pruneNonLinkedKeys();

        if (prunedSet.isEmpty() || (prunedSet.size() == 1 && prunedSet.iterator().next().size() == 0)) {
            // Give a second chance and try to get anything out of the predicates
            prunedSet = pruner.findMostSignificantDenominatorPredicate();
        }

        return prunedSet;
    }

    private List<List<KuduPredicate>> permutateScanSpec(List<List<KuduPredicate>> inputPermutationSets, KuduScanSpec scanSpec) {
        if (scanSpec.getPredicates().isEmpty() && scanSpec.getSubSets().isEmpty()) {
            return inputPermutationSets;
        }

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
            // This is AND, so add constraints to the current set
            for (List<KuduPredicate> list : inputPermutationSets) {
                List<KuduPredicate> newList = new ArrayList<>(list);
                for (KuduPredicate pred : scanSpec.getPredicates()) {
                    newList.add(pred);
                }
                newLists.add(newList);
            }

            for (KuduScanSpec subSet : scanSpec.getSubSets()) {
                newLists = permutateScanSpec(newLists, subSet);
            }
        }

        return newLists;
    }


    /**
     * Rebuilds (or fattens) the scan basing on permutation sets.
     * The scan is now essentially a set of permutations, where each permutation is a collection of AND-bound queries
     * and the OR of them is being taken into the output
     */
    public KuduScanSpec rebuildScanSpec(List<List<KuduPredicate>> permutationSet) {
        KuduScanSpec spec = new KuduScanSpec(input.getTableName());

        if (permutationSet.size() == 1) {
            // Just to keep things clear, lets not put it deeper unnecessarily
            spec.getPredicates().addAll(permutationSet.iterator().next());
        } else {
            for (List<KuduPredicate> permutation : permutationSet) {
                KuduScanSpec subScan = new KuduScanSpec(input.getTableName());
                subScan.getPredicates().addAll(permutation);
                spec.addSubSet(subScan);
            }
            spec.setPushOr(true);
        }

        return spec;
    }

}
