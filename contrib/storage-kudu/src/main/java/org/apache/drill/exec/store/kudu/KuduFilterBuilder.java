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

import com.google.common.collect.ImmutableList;
import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;

import java.util.Arrays;

import org.kududb.ColumnSchema;
import org.kududb.client.Bytes;
import org.kududb.client.ColumnRangePredicate;

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
            /// FXIEM: WHY skipping CompareFunctionsProcessor ?
            if (processor.isSuccess()) {
                nodeScanSpec = createKuduScanSpec(call, processor);
            }
        } else {
            switch (functionName) {
                case "booleanAnd":
                case "booleanOr":
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
        mergedSpec.getPredicates().addAll(leftScanSpec.getPredicates());
        mergedSpec.getPredicates().addAll(rightScanSpec.getPredicates());

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
        ColumnRangePredicate pred = new ColumnRangePredicate(colSchema);

        boolean isNullTest = false;
        byte[] startRow = null;
        byte[] stopRow = null;
        switch (functionName) {
            case "equal":
                pred.setLowerBound(((int) originalValue));
                pred.setUpperBound(((int) originalValue));
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
                    pred.setLowerBound((int) originalValue);
                } else {
                    pred.setUpperBound((int) originalValue);
                }
                break;
            case "greater_than":
                if (sortOrderAscending) {
                    // Not that great
                    pred.setUpperBound((int) originalValue);
                } else {
                    // Not that great
                    pred.setLowerBound((int) originalValue);
                }
                break;
            case "less_than_or_equal_to":
                if (sortOrderAscending) {
                    pred.setUpperBound((int) originalValue);
                } else {
                    pred.setLowerBound((int) originalValue);
                }
                break;
            case "less_than":
                if (sortOrderAscending) {
                    // Not that great
                    pred.setUpperBound((int) originalValue);
                } else {
                    // Not that great
                    pred.setLowerBound((int) originalValue);
                }
                break;
            case "isnull":
            case "isNull":
            case "is null":
                // Not supported
                break;
            case "isnotnull":
            case "isNotNull":
            case "is not null":
                // Not supported
                break;
            case "like":
                // Not supported
                break;
        }

        if (pred.getLowerBound() != null || pred.getUpperBound() != null) {
            return new KuduScanSpec(groupScan.getTableName(), groupScan.getKuduScanSpec().getKuduTableSchema(), pred);
        }
        // else
        return null;
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