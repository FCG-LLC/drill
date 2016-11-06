package org.apache.drill.exec.store.kudu;

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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Type;
import org.apache.kudu.client.Bytes;
import org.apache.kudu.client.ColumnRangePredicate;
import org.apache.kudu.client.KuduPredicate;

public class KuduFilterBuilder extends AbstractExprVisitor<KuduScanSpec, Void, RuntimeException> {

    final private KuduGroupScan groupScan;

    final private LogicalExpression le;

    private boolean allExpressionsConverted = true;

    private static Boolean nullComparatorSupported = null;

    KuduFilterBuilder(KuduGroupScan groupScan, LogicalExpression le) {
        this.groupScan = groupScan;
        this.le = le;
    }

    public KuduScanSpec parseTree() {
        KuduScanSpec parsedSpec = le.accept(this, null);
        if (parsedSpec != null) {
            parsedSpec = mergeScanSpecs("booleanAnd", this.groupScan.getKuduScanSpec(), parsedSpec);
        }
        return parsedSpec;
    }

    public boolean isAllExpressionsConverted() {
        // That's very nice optimization, but it will probably never work right here
        //return allExpressionsConverted;
        return false;
    }

    @Override
    public KuduScanSpec visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
        allExpressionsConverted = false;
        return null;
    }

    @Override
    public KuduScanSpec visitBooleanOperator(BooleanOperator op, Void value) throws RuntimeException {
        return visitFunctionCall(op, value);
    }

    @Override
    public KuduScanSpec visitFunctionCall(FunctionCall call, Void value) throws RuntimeException {
        KuduScanSpec nodeScanSpec = null;
        String functionName = call.getName();
        ImmutableList<LogicalExpression> args = call.args;

        if (CompareFunctionsProcessor.isCompareFunction(functionName)) {
            CompareFunctionsProcessor processor = CompareFunctionsProcessor.process(call, true);
            /// FIXME: WHY skipping CompareFunctionsProcessor ?
            if (processor.isSuccess()) {
                nodeScanSpec = createKuduScanSpec(call, processor);
            }
        } else {
            switch (functionName) {
                case "booleanAnd":
                //case "booleanOr":
                    KuduScanSpec firstScanSpec = args.get(0).accept(this, null);
                    for (int i = 1; i < args.size(); ++i) {
                        KuduScanSpec nextScanSpec = args.get(i).accept(this, null);
                        if (firstScanSpec != null && nextScanSpec != null) {
                            nodeScanSpec = mergeScanSpecs(functionName, firstScanSpec, nextScanSpec);
                        } else {
                            allExpressionsConverted = false;
                            if ("booleanAnd".equals(functionName)) {
                                nodeScanSpec = firstScanSpec == null ? nextScanSpec : firstScanSpec;
                            }
                        }
                        firstScanSpec = nodeScanSpec;
                    }
                    break;
            }
        }

        if (nodeScanSpec == null) {
            allExpressionsConverted = false;
        }

        return nodeScanSpec;
    }

    private KuduScanSpec mergeScanSpecs(String functionName, KuduScanSpec leftScanSpec, KuduScanSpec rightScanSpec) {
        KuduScanSpec mergedSpec = new KuduScanSpec(leftScanSpec.getTableName(), leftScanSpec.getKuduTableSchema());

        for (List<KuduPredicate> predicateList : Arrays.asList(leftScanSpec.getPredicates(), rightScanSpec.getPredicates())) {
            for (KuduPredicate pred : predicateList) {
                // Kudu API will handle merging predicates
                mergedSpec.getPredicates().add(pred);
            }
        }

        return mergedSpec;
    }

    private KuduScanSpec createKuduScanSpec(FunctionCall call, CompareFunctionsProcessor processor) {
        String functionName = processor.getFunctionName();
        SchemaPath field = processor.getPath();
        byte[] fieldValue = processor.getValue();
        Object originalValue = processor.getOriginalValue();
        boolean sortOrderAscending = processor.isSortOrderAscending();

        // FIXME: isRowKey should tell if a given column is the primary key of the current table
        boolean isRowKey = false;
//        if (!(isRowKey
//                || (!field.getRootSegment().isLastPath()
//                && field.getRootSegment().getChild().isLastPath()
//                && field.getRootSegment().getChild().isNamed())
//        )
//                ) {
//      /*
//       * if the field in this function is neither the row_key nor a qualified column, return.
//       */
//            return null;
//        }

        // FIXME: that's a good idea for Kudu too probably - create partial that would filter by prefix
//        if (processor.isRowKeyPrefixComparison()) {
//            return createRowKeyPrefixScanSpec(call, processor);
//        }


        String colName = field.getRootSegment().getPath();
        ColumnSchema colSchema = groupScan.getKuduScanSpec().getKuduTableSchema().getColumn(colName);

        // In case of String only:
        if (colSchema.getType() == Type.STRING && originalValue == null && fieldValue != null) {
            originalValue = new String(fieldValue);
        }

        boolean isNullTest = false;
        byte[] startRow = null;
        byte[] stopRow = null;

        KuduPredicate.ComparisonOp op = null;
        switch (functionName) {
            case "equal":
                op = KuduPredicate.ComparisonOp.EQUAL;
                if (isRowKey) {
                    startRow = fieldValue;
                    stopRow = Arrays.copyOf(fieldValue, fieldValue.length+1);
                }
                break;
            case "not_equal":
                // not supported
                break;
            case "greater_than_or_equal_to":
                if (sortOrderAscending) {
                    op = KuduPredicate.ComparisonOp.GREATER_EQUAL;
                } else {
                    op = KuduPredicate.ComparisonOp.LESS_EQUAL;
                }
                break;
            case "greater_than":
                if (sortOrderAscending) {
                    op = KuduPredicate.ComparisonOp.GREATER;
                } else {
                    op = KuduPredicate.ComparisonOp.LESS;
                }
                break;
            case "less_than_or_equal_to":
                if (sortOrderAscending) {
                    op = KuduPredicate.ComparisonOp.LESS_EQUAL;
                } else {
                    op = KuduPredicate.ComparisonOp.GREATER_EQUAL;
                }
                break;
            case "less_than":
                if (sortOrderAscending) {
                    // Not that great
                    op = KuduPredicate.ComparisonOp.LESS;
                } else {
                    // Not that great
                    op = KuduPredicate.ComparisonOp.GREATER;
                }
                break;
            case "isnull":
            case "isNull":
            case "is null":
                // Not supported
                // op = KuduPredicate.ComparisonOp.EQUAL;
                break;
            case "isnotnull":
            case "isNotNull":
            case "is not null":
                // Not directly supported - could be done though with max value limits for some types
                break;
            case "like":
                String likeConstraint = (String) originalValue;
                if (!likeConstraint.startsWith("%")) {
                    // Prefix search supported only

                    if (likeConstraint.indexOf('%') == -1) {
                        // This is equals query
                        op = KuduPredicate.ComparisonOp.EQUAL;
                    } else {
                        String strMin = likeConstraint.split("%")[0];
                        String strMax = strMin.substring(0, strMin.length()-1);
                        strMax = strMax + ((char) (strMin.charAt(strMin.length()-1)+1)); // FIXME: how this works? 255 character anyone?

                        KuduPredicate minPredicate = KuduPredicate.newComparisonPredicate(colSchema, KuduPredicate.ComparisonOp.GREATER_EQUAL, strMin);
                        KuduPredicate maxPredicate = KuduPredicate.newComparisonPredicate(colSchema, KuduPredicate.ComparisonOp.LESS, strMax);

                        return new KuduScanSpec(
                                groupScan.getTableName(),
                                groupScan.getKuduScanSpec().getKuduTableSchema(),
                                Arrays.asList(minPredicate, maxPredicate)
                        );
                    }
                }
                // Not supported - could be supporting prefix search
                break;



        }

        KuduPredicate predicate = new KuduPredicateFactory(colSchema, op).create(originalValue);

        if (predicate != null) {
            return new KuduScanSpec(groupScan.getTableName(), groupScan.getKuduScanSpec().getKuduTableSchema(), predicate);
        }

        return null;
    }

    /**
     * Delegates setting upport and lower bound depending on the actual type
     */
    public static class KuduPredicateFactory {
        private ColumnSchema columnSchema;
        private KuduPredicate.ComparisonOp comparisonOp;

        public KuduPredicateFactory(ColumnSchema columnSchema, KuduPredicate.ComparisonOp comparisonOp) {
            this.columnSchema = columnSchema;
            this.comparisonOp = comparisonOp;
        }

        public KuduPredicate create(Object originalValue) {
            if (originalValue == null || comparisonOp == null) {
                return null;
            }

            switch(this.columnSchema.getType()) {
                case BINARY:
                    return KuduPredicate.newComparisonPredicate(columnSchema, comparisonOp, (byte[]) originalValue);
                case INT8:
                    return KuduPredicate.newComparisonPredicate(columnSchema, comparisonOp, ((Integer) originalValue).byteValue());
                case INT16:
                    return KuduPredicate.newComparisonPredicate(columnSchema, comparisonOp, ((Integer) originalValue).shortValue());
                case INT32:
                    return KuduPredicate.newComparisonPredicate(columnSchema, comparisonOp, (int) originalValue);
                case INT64:
                    if (originalValue instanceof Integer) {
                        return KuduPredicate.newComparisonPredicate(columnSchema, comparisonOp, ((Integer) originalValue).longValue());
                    } else {
                        return KuduPredicate.newComparisonPredicate(columnSchema, comparisonOp, (long) originalValue);
                    }
                case BOOL:
                    return KuduPredicate.newComparisonPredicate(columnSchema, comparisonOp, (boolean) originalValue);
                case DOUBLE:
                    return KuduPredicate.newComparisonPredicate(columnSchema, comparisonOp, (double) originalValue);
                case FLOAT:
                    return KuduPredicate.newComparisonPredicate(columnSchema, comparisonOp, (float) originalValue);
                case STRING:
                    return KuduPredicate.newComparisonPredicate(columnSchema, comparisonOp, (String) originalValue);
                case UNIXTIME_MICROS:
                    return KuduPredicate.newComparisonPredicate(columnSchema, comparisonOp, ((Date) originalValue).getTime()*1000L);
            }

            return null;
        }

    }

//    private HBaseScanSpec createRowKeyPrefixScanSpec(FunctionCall call,
//                                                     CompareFunctionsProcessor processor) {
//        byte[] startRow = processor.getRowKeyPrefixStartRow();
//        byte[] stopRow  = processor.getRowKeyPrefixStopRow();
//        Filter filter   = processor.getRowKeyPrefixFilter();
//
//        if (startRow != HConstants.EMPTY_START_ROW ||
//                stopRow != HConstants.EMPTY_END_ROW ||
//                filter != null) {
//            return new HBaseScanSpec(groupScan.getTableName(), startRow, stopRow, filter);
//        }
//
//        // else
//        return null;
//    }

}