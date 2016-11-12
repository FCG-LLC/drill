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
import org.apache.kudu.Common;
import org.apache.kudu.Schema;
import org.apache.kudu.client.KuduPredicate;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class KuduScanSpecOptimizer {
    private KuduScanSpec input;
    private List<String> primaryKeys;
    private Schema schema;

    public KuduScanSpecOptimizer(KuduScanSpec input, Schema schema) {
        this.input = input;

        primaryKeys = new ArrayList<>();
        for (ColumnSchema columnSchema : schema.getPrimaryKeyColumns()) {
            primaryKeys.add(columnSchema.getName());
        }

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

        NonPrimaryKeyPermutationPruner pruner = new NonPrimaryKeyPermutationPruner(basePermutationSet);
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
                Set<KuduPredicate> linkedPrimaryKeyPredicates;
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

        private int predicateColumnPosition(KuduPredicate pred) {
            return schema.getColumnIndex(pred.toPB().getColumn());
        }

        private byte[] getPredicateLowerValue(KuduPredicate pred) {
            Common.ColumnPredicatePB pb = pred.toPB();
            switch (pb.getPredicateCase()) {
                case EQUALITY:
                    return pb.getEquality().getValue().toByteArray();
                case RANGE:
                    return pb.getRange().getLower().toByteArray();
            }
            return new byte[]{};
        }

        private byte[] getPredicateUpperValue(KuduPredicate pred) {
            Common.ColumnPredicatePB pb = pred.toPB();
            switch (pb.getPredicateCase()) {
                case EQUALITY:
                    return pb.getEquality().getValue().toByteArray();
                case RANGE:
                    return pb.getRange().getUpper().toByteArray();
            }
            return new byte[]{};
        }

        private int compareBytes(byte[] b1, byte[] b2) {
            // Very very naive
            for (int i = 0; i < Math.max(b1.length, b2.length); i++) {
                if (b1.length == i)
                    return -1;
                if (b2.length == i) {
                    return 1;
                }

                if (b1[i] != b2[i]) {
                    return ((int) b1[i])-((int) b2[i]);
                }
            }

            return 0;
        }

        /**
         * In case everything was pruned, see if we can still come with
         * a reasonable scan without including first few primary key elements
         */
        public List<List<KuduPredicate>> findMostSignificantDenominatorPredicate() {
            // Iteration one - sort the predicates within each set
            for (List<KuduPredicate> permutation : permutationSet) {
                Collections.sort(permutation, new Comparator<KuduPredicate>() {
                    @Override
                    public int compare(KuduPredicate o1, KuduPredicate o2) {
                        int pos1 = predicateColumnPosition(o1);
                        int pos2 = predicateColumnPosition(o2);

                        // Simple
                        if (pos1 != pos2) {
                            return pos1 - pos2;
                        }

                        // Not so simple
                        byte[] str1Lower = getPredicateLowerValue(o1);
                        byte[] str1Upper = getPredicateUpperValue(o1);
                        byte[] str2Lower = getPredicateLowerValue(o2);
                        byte[] str2Upper = getPredicateUpperValue(o2);

                        // Very naive
                        if (Arrays.equals(str1Lower, str2Lower)) {
                            if (Arrays.equals(str1Upper, str2Upper)) {
                                return 0;
                            } else {
                                return compareBytes(str1Upper, str2Upper);
                            }
                        } else {
                            return compareBytes(str1Lower, str2Lower);
                        }
                    }
                });
            }

            // Iteration two - check how far can we gat away with single permutation
            List<KuduPredicate> singlePermutation = new ArrayList<>();

            int indexPosition = 0;

            do {
                KuduPredicate candidate = null;

                for (List<KuduPredicate> permutation : permutationSet) {
                    if (permutation.size() <= indexPosition) {
                        // This set is too long already - finish
                        return Arrays.asList(singlePermutation);
                    }

                    KuduPredicate pred = permutation.get(indexPosition);

                    if (candidate == null) {
                        candidate = pred;
                    } else {
                        if (!candidate.equals(pred)) {
                            // This is the base we came with
                            return Arrays.asList(singlePermutation);
                        }
                    }
                }

                // Still here? Great!
                if (candidate != null) {
                    singlePermutation.add(candidate);
                }

                ++indexPosition;
            } while(true);
        }
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
