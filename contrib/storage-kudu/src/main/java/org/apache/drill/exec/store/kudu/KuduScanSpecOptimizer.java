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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;


public class KuduScanSpecOptimizer {
    private KuduScanSpec input;
    private List<String> primaryKeys;

    public KuduScanSpecOptimizer(KuduScanSpec input, Schema schema) {
        this.input = input;

        primaryKeys = new ArrayList<>();
        for (ColumnSchema columnSchema : schema.getPrimaryKeyColumns()) {
            primaryKeys.add(columnSchema.getName());
        }
    }

    private Set<String> findGaps(List<KuduPredicate> predicates) {
        Set<String> leftCols = new HashSet<String>();
        for (KuduPredicate pred : predicates) {
            leftCols.add(pred.toPB().getColumn());
        }

        for (String primaryKey : primaryKeys) {
            if (leftCols.contains(primaryKey)) {
                // Good, we have it, so far it's valid, lets move to the next...
                leftCols.remove(primaryKey);
            } else {
                // We have a gap - whatever is left should not be included on any path within an OR query
                return leftCols;
            }
        }

        return leftCols;
    }

    /**
     * Rebuilds the scan spec, removing anything that could effect in inefficient OR query
     * (not having full path throguh primary key columns)
     * @deprecated  NO, don't use it - clean this up
     */
    public KuduScanSpec pruneScanSpec(KuduScanSpec input) {
        // This is simplistic approach, which makes global view on the predicates and constraints
        // and doesn't take into account alternative paths to get some results
        KuduScanSpec prunedScanSpec = input;
        List<KuduScanSpec> prunedScanSpecs;

        do {
            List<List<KuduPredicate>> permutationSet = permutateScanSpec(prunedScanSpec);

            Set<String> columnsForbiddenInORPath = new HashSet<>();
            for (List<KuduPredicate> predicates : permutationSet) {
                columnsForbiddenInORPath.addAll(findGaps(predicates));
            }

            // Do the actual pruning
            prunedScanSpecs = new ArrayList<>();
            prunedScanSpec = rebuildScanSpec(prunedScanSpec, false, columnsForbiddenInORPath, prunedScanSpecs);
        } while (!prunedScanSpecs.isEmpty());

        return prunedScanSpec;
    }

    /**
     *  We need to check if for leafs following conditions are valid:
     *    (a) there is no "OR" in the path, or
     *    (b) there is "OR" in the path and primary items up until the one occurring in OR are included
     *       i.e. if we have primary key: k1, k2, k3, k4
     *       following are OK:
     *         k1 = 2 OR k1 = 3
     *         k1 = 2 AND (k2 = 'a' OR k2 = 'b')
     *         k1 = 2 AND (k2 = 'a' OR k2 = 'b') AND k4 = 100
     *       following are NOT:
     *         k2 = 'a' OR k2 = 'b'
     *         k1 = 2 AND (k2 = 'a' OR k3 = 99999)
     */

    private KuduScanSpec rebuildScanSpec(KuduScanSpec cur, boolean parentIsOnORPath, Set<String> columnsForbiddenInORPath, List<KuduScanSpec> prunedScanSpecs) {
        boolean isOnORPath = cur.isPushOr() || parentIsOnORPath;

        KuduScanSpec out = new KuduScanSpec(cur.getTableName());

        for (KuduPredicate pred : cur.getPredicates()) {
            if (isOnORPath && columnsForbiddenInORPath.contains(pred.toPB().getColumn())) {
                // We skip this one, sorry
                prunedScanSpecs.add(cur);
                return out;
            }
        }

        out.getPredicates().addAll(cur.getPredicates());
        out.setPushOr(cur.isPushOr());

        for (KuduScanSpec child : cur.getSubSets()) {
            KuduScanSpec rebuiltScan = rebuildScanSpec(child, isOnORPath, columnsForbiddenInORPath, prunedScanSpecs);
            if (!rebuiltScan.isEmpty()) {
                out.getSubSets().add(rebuiltScan);
            }
        }

        return out;
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
    public List<List<KuduPredicate>> permutateScanSpec(KuduScanSpec scanSpec) {
        List<List<KuduPredicate>> inputPermutationSets = new ArrayList<>();
        inputPermutationSets.add(new ArrayList<KuduPredicate>());

        List<List<KuduPredicate>> basePermutationSet = permutateScanSpec(inputPermutationSets, scanSpec);

        return new NonPrimaryKeyPermutationPruner(basePermutationSet).pruneNonLinkedKeys();
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
     * We need to check if there are no alternative scans on non-priority key items
     * In some cases they are good, but in some they bring too much trouble. The actual case
     * depends on multiple factors and we keep it simple here, so use naive rule for pruning such
     * cases. This also means that with more complex structures, it won't really work.
     */
    private class NonPrimaryKeyPermutationPruner {
        private List<List<KuduPredicate>> permutationSet;

        public final static int MAX_NON_PRIMARY_KEY_ALTERNATIVES = 1;

        NonPrimaryKeyPermutationPruner(List<List<KuduPredicate>> permutationSet) {
            this.permutationSet = permutationSet;
        }

        private Set<String> findGaps(List<KuduPredicate> predicates) {
            Set<String> leftCols = new HashSet<String>();
            for (KuduPredicate pred : predicates) {
                leftCols.add(pred.toPB().getColumn());
            }

            for (String primaryKey : primaryKeys) {
                if (leftCols.contains(primaryKey)) {
                    // Good, we have it, so far it's valid, lets move to the next...
                    leftCols.remove(primaryKey);
                } else {
                    // We have a gap - whatever is left should not be included on any path within an OR query
                    return leftCols;
                }
            }

            return leftCols;
        }

        private Set<KuduPredicate> findLinkedPrimaryKeyPart(List<KuduPredicate> permutation) {
            Set<KuduPredicate> linkedPart = new HashSet<>();

            Map<String, KuduPredicate> leftCols = new HashMap<>();
            for (KuduPredicate pred : permutation) {
                leftCols.put(pred.toPB().getColumn(), pred);
            }

            for (String primaryKey : primaryKeys) {
                if (leftCols.keySet().contains(primaryKey)) {
                    // Good, we have it, so far it's valid, lets move to the next...
                    linkedPart.add(leftCols.remove(primaryKey));
                } else {
                    // We have a gap - whatever is left should not be included on any path within an OR query
                    return linkedPart;
                }
            }

            return linkedPart;
        }

        public List<List<KuduPredicate>> pruneNonLinkedKeys() {
            List<List<KuduPredicate>> prunedPermutations = new ArrayList<>();

            Map<Set<KuduPredicate>, List<Set<KuduPredicate>>> linkedPrimaryKeyPartToRest = new HashMap<>();

            for (List<KuduPredicate> permutation : permutationSet) {
                // If KuduPredicate would implement Comparable then TreeSet could have been better a choice
                Set<KuduPredicate> linkedPrimaryKeyPredicates = new HashSet<>();
                Set<KuduPredicate> nonLinkedPredicates = new HashSet<>();

                linkedPrimaryKeyPredicates = findLinkedPrimaryKeyPart(permutation);
                for (KuduPredicate pred : permutation) {
                    if (!linkedPrimaryKeyPredicates.contains(pred)) {
                        nonLinkedPredicates.add(pred);
                    }
                }

                if (nonLinkedPredicates.isEmpty()) {
                    // Just pass the current permutation - it's safe
                    prunedPermutations.add(permutation);
                } else {
                    List<Set<KuduPredicate>> list = linkedPrimaryKeyPartToRest.get(linkedPrimaryKeyPredicates);

                    if (list == null) {
                        list = new ArrayList<>();
                        linkedPrimaryKeyPartToRest.put(linkedPrimaryKeyPredicates, list);
                    }
                    list.add(nonLinkedPredicates);
                }
            }

            // For any permutation of concern, see how many filters are there
            for (Set<KuduPredicate> linkedKeyPart : linkedPrimaryKeyPartToRest.keySet()) {
                List<Set<KuduPredicate>> nonLinkedKeySets = linkedPrimaryKeyPartToRest.get(linkedKeyPart);
                if (nonLinkedKeySets.size() > MAX_NON_PRIMARY_KEY_ALTERNATIVES) {
                    // Skip the permutations
                    prunedPermutations.add(new ArrayList<>(linkedKeyPart));
                } else {
                    // Include the permutations
                    for (Set<KuduPredicate> nonPrimaryKeyPredicates : nonLinkedKeySets) {
                        List<KuduPredicate> permutation = new ArrayList<>(linkedKeyPart);
                        permutation.addAll(nonPrimaryKeyPredicates);

                        prunedPermutations.add(permutation);
                    }
                }
            }

            return prunedPermutations;
        }
    }
}
