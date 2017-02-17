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
import java.util.Date;
import java.util.List;

import org.apache.drill.exec.store.ischema.Records;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduPredicate;

public class KuduFilterBuilder extends AbstractExprVisitor<KuduScanSpec, Void, RuntimeException> {

    final private KuduGroupScan groupScan;
    final private Schema tableSchema;

    final private LogicalExpression le;

    KuduFilterBuilder(KuduGroupScan groupScan, LogicalExpression le) {
        this.groupScan = groupScan;
        this.tableSchema = groupScan.getTableSchema();
        this.le = le;
    }

    public KuduScanSpec parseTree() {
        KuduScanSpec parsedSpec = le.accept(this, null);
        if (parsedSpec != null) {
            if (parsedSpec.isPushOr()) {
                parsedSpec = mergeScanSpecs("booleanOr", this.groupScan.getKuduScanSpec(), parsedSpec);
            } else {
                parsedSpec = mergeScanSpecs("booleanAnd", this.groupScan.getKuduScanSpec(), parsedSpec);
            }
        }
        return parsedSpec;
    }

    public boolean isAllExpressionsConverted() {
        // That would be a very nice optimization, but it will probably never work right here
        return false;
    }

    @Override
    public KuduScanSpec visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
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
                            if ("booleanAnd".equals(functionName)) {
                                nodeScanSpec = firstScanSpec == null ? nextScanSpec : firstScanSpec;
                            }
                        }
                        firstScanSpec = nodeScanSpec;
                    }
                    break;
            }
        }

        return nodeScanSpec;
    }

    private KuduScanSpec mergeScanSpecs(String functionName, KuduScanSpec leftScanSpec, KuduScanSpec rightScanSpec) {
        KuduScanSpec mergedSpec = new KuduScanSpec(leftScanSpec.getTableName());

        switch (functionName) {
            case "booleanAnd":
                for (KuduScanSpec scanSpec : Arrays.asList(leftScanSpec, rightScanSpec)) {
                    if (scanSpec.isPushOr()) {
                        mergedSpec.addSubSet(scanSpec);
                    } else {
                        // Kudu API will handle merging predicates
                        mergedSpec.getPredicates().addAll(scanSpec.getPredicates());
                        mergedSpec.getSubSets().addAll(scanSpec.getSubSets());
                    }
                }
                break;
            case "booleanOr":
                mergedSpec.setPushOr(true);

                for (KuduScanSpec scanSpec : Arrays.asList(leftScanSpec, rightScanSpec)) {
                    if (scanSpec.isPushOr() || (leftScanSpec.isAtomic() && rightScanSpec.isAtomic()) || (leftScanSpec.isAtomic() && rightScanSpec.isPushOr()) || (leftScanSpec.isPushOr() && rightScanSpec.isAtomic())) { // FIXME: or single element?
                        // This is simple, we just add its fields
                        mergedSpec.getPredicates().addAll(scanSpec.getPredicates());
                        mergedSpec.getSubSets().addAll(scanSpec.getSubSets());
                    } else {
                        // This means it is a separate set of constraints...
                        mergedSpec.addSubSet(scanSpec);
                    }
                }
                break;
        }

        return mergedSpec;
    }

    private KuduScanSpec createKuduScanSpec(FunctionCall call, CompareFunctionsProcessor processor) {
        String functionName = processor.getFunctionName();
        SchemaPath field = processor.getPath();
        byte[] fieldValue = processor.getValue();
        Object originalValue = processor.getOriginalValue();
        boolean sortOrderAscending = processor.isSortOrderAscending();


        String colName = field.getRootSegment().getPath();
        ColumnSchema colSchema = tableSchema.getColumn(colName);

        // In case of String only:
        if (colSchema.getType() == Type.STRING && originalValue == null && fieldValue != null) {
            originalValue = new String(fieldValue);
        }

        KuduPredicate.ComparisonOp op = null;
        switch (functionName) {
            case "equal":
                op = KuduPredicate.ComparisonOp.EQUAL;
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
                                Arrays.asList(minPredicate, maxPredicate)
                        );
                    }
                }
                // Not supported - could be supporting prefix search
                break;

        }


        // If this are unsigned integers, we must build up to two predicates, unless this was IN or = or <>
        if (isInequalityOp(op) && isUnsignedIntColumn(colSchema)) {
            // We convert the inequality for unsigned integer (logical) into two inequalities for signed integer (physical on Kudu)
            // E.g. we have INT8 and inequality e.g. WHERE x >= 45
            // This will be broken down into conditions:
            // x >= 45 OR x < 0
            // Case 2: WHERE x >= 185
            // x >= -... AND x < 0
            // Case 3: WHERE x < 5
            // x >= 0 AND x < 5
            // Case 4: WHERE x <= 185
            // x <= -...OR x >= 0

            switch (op) {
                case GREATER:
                case GREATER_EQUAL:
                    if (((int) originalValue) <= maxSignedValue(colSchema)) {
                        return new KuduScanSpec(groupScan.getTableName(), Arrays.asList(
                                new KuduPredicateFactory(colSchema, op).create(originalValue),
                                new KuduPredicateFactory(colSchema, KuduPredicate.ComparisonOp.LESS).create(0)
                        ), true);
                    } else {
                        return new KuduScanSpec(groupScan.getTableName(), Arrays.asList(
                                new KuduPredicateFactory(colSchema, op).create(originalValue),
                                new KuduPredicateFactory(colSchema, KuduPredicate.ComparisonOp.LESS).create(0)
                        ), false);
                    }
                case LESS:
                case LESS_EQUAL:
                    if (((int) originalValue) <= maxSignedValue(colSchema)) {
                        return new KuduScanSpec(groupScan.getTableName(), Arrays.asList(
                                new KuduPredicateFactory(colSchema, op).create(originalValue),
                                new KuduPredicateFactory(colSchema, KuduPredicate.ComparisonOp.GREATER_EQUAL).create(0)
                        ), false);
                    } else {
                        return new KuduScanSpec(groupScan.getTableName(), Arrays.asList(
                                new KuduPredicateFactory(colSchema, op).create(originalValue),
                                new KuduPredicateFactory(colSchema, KuduPredicate.ComparisonOp.GREATER_EQUAL).create(0)
                        ), true);
                    }
                default:
                    return null;
            }
        } else {
            KuduPredicate predicate = new KuduPredicateFactory(colSchema, op).create(originalValue);

            if (predicate != null) {
                return new KuduScanSpec(groupScan.getTableName(), predicate);
            }
        }

        return null;
    }

    private boolean isUnsignedIntColumn(ColumnSchema columnSchema) {
        switch (columnSchema.getType()) {
            case INT8:
                return groupScan.getStorageConfig().isAllUnsignedINT8();
            case INT16:
                return groupScan.getStorageConfig().isAllUnsignedINT16();
            default:
                return false;
        }
    }

    private boolean isInequalityOp(KuduPredicate.ComparisonOp op) {
        if (op == null) {
            return false;
        }

        switch (op) {
            case GREATER:
            case GREATER_EQUAL:
            case LESS:
            case LESS_EQUAL:
                return true;
            default:
                return false;
        }
    }

    private int maxSignedValue(ColumnSchema columnSchema) {
        switch (columnSchema.getType()) {
            case INT8:
                return Byte.MAX_VALUE;
            case INT16:
                return Short.MAX_VALUE;
            case INT32:
                return Integer.MAX_VALUE;
            default:
                throw new UnsupportedOperationException("Type " + columnSchema.getType() + " for column "+columnSchema.getName()+" is not supported here");
        }
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

}