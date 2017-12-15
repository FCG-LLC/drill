package org.apache.drill.exec.store.kudu;


import org.apache.kudu.Common;
import org.apache.kudu.client.KuduPredicate;

import java.util.*;

/**
 * We need to check if there are no alternative scans on non-priority key items
 * In some cases they are good, but in some they bring too much trouble. The actual case
 * depends on multiple factors and we keep it simple here, so use naive rule for pruning such
 * cases. This also means that with more complex structures, it won't really work.
 */
public class NonPrimaryKeyPermutationPruner {

    private KuduUtils kuduUtils;

    private List<List<KuduPredicate>> permutationSet;
    private KuduScanSpecOptimizer kuduScanSpecOptimizer;

    public NonPrimaryKeyPermutationPruner(KuduUtils kuduUtils, KuduScanSpecOptimizer kuduScanSpecOptimizer, List<List<KuduPredicate>> permutationSet) {
        this.kuduScanSpecOptimizer = kuduScanSpecOptimizer;
        this.permutationSet = permutationSet;
        this.kuduUtils = kuduUtils;
    }

    private Map<String, List<KuduPredicate>> toPredicatesByColumn(List<KuduPredicate> permutation) {
        Map<String, List<KuduPredicate>> predicatesByColumn = new HashMap<>();
        for (KuduPredicate pred : permutation) {
            String column = pred.toPB().getColumn();
            List<KuduPredicate> columnPredicates = predicatesByColumn.get(column);
            if (columnPredicates == null) {
                columnPredicates = new ArrayList<>();
                predicatesByColumn.put(column, columnPredicates);
            }
            columnPredicates.add(pred);
        }
        return predicatesByColumn;
    }

    private Set<KuduPredicate> findLinkedPrimaryKeyPart(List<KuduPredicate> permutation) {
        Set<KuduPredicate> linkedPart = new HashSet<>();

        Map<String, List<KuduPredicate>> predicatesByColumn = toPredicatesByColumn(permutation);

        for (String primaryKey : kuduScanSpecOptimizer.primaryKeys) {
            if (predicatesByColumn.keySet().contains(primaryKey)) {
                // Good, we have it, so far it's valid, lets move to the next...
                linkedPart.addAll(predicatesByColumn.remove(primaryKey));
            } else {
                // We have a gap - whatever is left should not be included on any path within an OR query
                return linkedPart;
            }
        }

        return linkedPart;
    }

    private Map<Set<KuduPredicate>, List<Set<KuduPredicate>>> prepLinkedPrimaryKeyPartToRestMap(
        List<List<KuduPredicate>> prunedPermutations) {
        Map<Set<KuduPredicate>, List<Set<KuduPredicate>>> linkedPrimaryKeyPartToRest = new HashMap<>();

        for (List<KuduPredicate> permutation : permutationSet) {
            // If KuduPredicate would implement Comparable then TreeSet could
            // have been better a choice
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
                List<Set<KuduPredicate>> list = linkedPrimaryKeyPartToRest
                    .get(linkedPrimaryKeyPredicates);

                if (list == null) {
                    list = new ArrayList<>();
                    linkedPrimaryKeyPartToRest.put(linkedPrimaryKeyPredicates, list);
                }
                list.add(nonLinkedPredicates);
            }
        }
        return linkedPrimaryKeyPartToRest;
    }

    /**
     * Returns true if this pruner has permutationSet with only one predicate and it's NONE.
     * @return
     */
    public boolean isNone() {
        if (permutationSet == null || permutationSet.size() != 1) {
            return false;
        }
        List<KuduPredicate> permutation = permutationSet.get(0);
        return permutation.size() == 1 && kuduUtils.findFirstNone(permutation) != null;
    }

   /**
     * Reduces/removes the predicates with KuduPredicate.Type.NONE
     * according to the following logical laws:
     * <pre>
     * A and B and NONE and ... => NONE
     * A or B or NONE or ... =>  A or B or ...
     * </pre>
     * We can do such reduction, because the {@link #permutationSet} top list is an alternative
     * and the inner lists are conjunctions.
     */
    public List<List<KuduPredicate>> reduceNonePredicates() {
      // NOTE: We have to catch a sample NONE predicate (column does not matter, only the type=NONE),
      // because the designers of KuduClient library made it's constructors hidden...
      KuduPredicate nonePredicate = null;
      if (!permutationSet.isEmpty()) {
          List<List<KuduPredicate>> newPermutationSet = new ArrayList<>();
          for (List<KuduPredicate> conjunction : permutationSet) {
              KuduPredicate foundNone = kuduUtils.findFirstNone(conjunction);
              if (foundNone == null) {
                  newPermutationSet.add(conjunction);
              } else if (nonePredicate == null) {
                  nonePredicate = foundNone;
              }
          }
          if (newPermutationSet.isEmpty()) {
              // all are NONE, so just make it one big NONE predicate:
              List<KuduPredicate> nonePermutation = new ArrayList<>(1);
              nonePermutation.add(nonePredicate);
              newPermutationSet.add(nonePermutation);
          }
          permutationSet = newPermutationSet;
      }
      return permutationSet;
    }

    public List<List<KuduPredicate>> pruneNonLinkedKeys() {
        List<List<KuduPredicate>> prunedPermutations = new ArrayList<>();

        Map<Set<KuduPredicate>, List<Set<KuduPredicate>>> linkedPrimaryKeyPartToRest
          = prepLinkedPrimaryKeyPartToRestMap(prunedPermutations);

        // For any permutation of concern, see how many filters are there
        for (Set<KuduPredicate> linkedKeyPart : linkedPrimaryKeyPartToRest.keySet()) {
            List<Set<KuduPredicate>> nonLinkedKeySets = linkedPrimaryKeyPartToRest.get(linkedKeyPart);
            if (nonLinkedKeySets.size() > kuduScanSpecOptimizer.maxNonPrimaryKeyAlternatives) {
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
        return kuduScanSpecOptimizer.schema.getColumnIndex(pred.toPB().getColumn());
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
            if (b1.length == i) {
                return -1;
            }
            if (b2.length == i) {
                return 1;
            }

            if (b1[i] != b2[i]) {
                return (b1[i])-(b2[i]);
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
