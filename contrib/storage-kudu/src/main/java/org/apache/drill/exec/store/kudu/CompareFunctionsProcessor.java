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

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.CastExpression;
import org.apache.drill.common.expression.ConvertExpression;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;
import org.apache.htrace.fasterxml.jackson.annotation.JsonFormat;
import org.kududb.client.Bytes;

import java.nio.ByteOrder;


class CompareFunctionsProcessor extends AbstractExprVisitor<Boolean, LogicalExpression, RuntimeException> {
    private byte[] value;
    private Object originalValue;
    private boolean success;
    private boolean isEqualityFn;
    private SchemaPath path;
    private String functionName;
    private boolean sortOrderAscending;

    // Fields for row-key prefix comparison
    // If the query is on row-key prefix, we cannot use a standard template to identify startRow, stopRow and filter
    // Hence, we use these local variables(set depending upon the encoding type in user query)
    private boolean isRowKeyPrefixComparison;
    byte[] rowKeyPrefixStartRow;
    byte[] rowKeyPrefixStopRow;

    public static boolean isCompareFunction(String functionName) {
        return COMPARE_FUNCTIONS_TRANSPOSE_MAP.keySet().contains(functionName);
    }

    public static CompareFunctionsProcessor process(FunctionCall call, boolean nullComparatorSupported) {
        String functionName = call.getName();
        LogicalExpression nameArg = call.args.get(0);
        LogicalExpression valueArg = call.args.size() >= 2 ? call.args.get(1) : null;
        CompareFunctionsProcessor evaluator = new CompareFunctionsProcessor(functionName);

        if (valueArg != null) { // binary function
            if (VALUE_EXPRESSION_CLASSES.contains(nameArg.getClass())) {
                LogicalExpression swapArg = valueArg;
                valueArg = nameArg;
                nameArg = swapArg;
                evaluator.functionName = COMPARE_FUNCTIONS_TRANSPOSE_MAP.get(functionName);
            }
            evaluator.success = nameArg.accept(evaluator, valueArg);
        } else if (nullComparatorSupported && call.args.get(0) instanceof SchemaPath) {
            evaluator.success = true;
            evaluator.path = (SchemaPath) nameArg;
        }

        return evaluator;
    }

    public CompareFunctionsProcessor(String functionName) {
        this.success = false;
        this.functionName = functionName;
        this.isEqualityFn = COMPARE_FUNCTIONS_TRANSPOSE_MAP.containsKey(functionName)
                && COMPARE_FUNCTIONS_TRANSPOSE_MAP.get(functionName).equals(functionName);
        this.isRowKeyPrefixComparison = false;
        this.sortOrderAscending = true;
    }

    public byte[] getValue() {
        return value;
    }

    public Object getOriginalValue() {
        return originalValue;
    }

    public boolean isSuccess() {
        return success;
    }

    public SchemaPath getPath() {
        return path;
    }

    public String getFunctionName() {
        return functionName;
    }

    public boolean isRowKeyPrefixComparison() {
        return isRowKeyPrefixComparison;
    }

    public byte[] getRowKeyPrefixStartRow() {
        return rowKeyPrefixStartRow;
    }

    public byte[] getRowKeyPrefixStopRow() {
        return rowKeyPrefixStopRow;
    }

    public boolean isSortOrderAscending() {
        return sortOrderAscending;
    }

    @Override
    public Boolean visitCastExpression(CastExpression e, LogicalExpression valueArg) throws RuntimeException {
        if (e.getInput() instanceof CastExpression || e.getInput() instanceof SchemaPath) {
            return e.getInput().accept(this, valueArg);
        }
        return false;
    }

    @Override
    public Boolean visitConvertExpression(ConvertExpression e, LogicalExpression valueArg) throws RuntimeException {
        if (e.getConvertFunction() == ConvertExpression.CONVERT_FROM) {

            String encodingType = e.getEncodingType();
            int prefixLength    = 0;

            // Handle scan pruning in the following scenario:
            // The row-key is a composite key and the CONVERT_FROM() function has byte_substr() as input function which is
            // querying for the first few bytes of the row-key(start-offset 1)
            // Example WHERE clause:
            // CONVERT_FROM(BYTE_SUBSTR(row_key, 1, 8), 'DATE_EPOCH_BE') < DATE '2015-06-17'
            if (e.getInput() instanceof FunctionCall) {

                // We can prune scan range only for big-endian encoded data
                if (encodingType.endsWith("_BE") == false) {
                    return false;
                }

                FunctionCall call = (FunctionCall)e.getInput();
                String functionName = call.getName();
                if (!functionName.equalsIgnoreCase("byte_substr")) {
                    return false;
                }

                LogicalExpression nameArg = call.args.get(0);
                LogicalExpression valueArg1 = call.args.size() >= 2 ? call.args.get(1) : null;
                LogicalExpression valueArg2 = call.args.size() >= 3 ? call.args.get(2) : null;

                if (((nameArg instanceof SchemaPath) == false) ||
                        (valueArg1 == null) || ((valueArg1 instanceof ValueExpressions.IntExpression) == false) ||
                        (valueArg2 == null) || ((valueArg2 instanceof ValueExpressions.IntExpression) == false)) {
                    return false;
                }

                // FIXME: this was removed without much thought
                boolean isRowKey = false;
                //boolean isRowKey =((SchemaPath)nameArg).getAsUnescapedPath().equals(DrillHBaseConstants.ROW_KEY);

                int offset = ((ValueExpressions.IntExpression)valueArg1).getInt();

                if (!isRowKey || (offset != 1)) {
                    return false;
                }

                this.path    = (SchemaPath)nameArg;
                prefixLength = ((ValueExpressions.IntExpression)valueArg2).getInt();
                this.isRowKeyPrefixComparison = true;
                // FIXME: another thing removed
                return false;
                //return visitRowKeyPrefixConvertExpression(e, prefixLength, valueArg);
            }

            if (e.getInput() instanceof SchemaPath) {
                ByteBuf bb = null;
                switch (encodingType) {
                    case "INT_BE":
                    case "INT":
                    case "UINT_BE":
                    case "UINT":
                    case "UINT4_BE":
                    case "UINT4":
                        if (valueArg instanceof ValueExpressions.IntExpression
                                && (isEqualityFn || encodingType.startsWith("U"))) {
                            bb = newByteBuf(4, encodingType.endsWith("_BE"));
                            bb.writeInt(((ValueExpressions.IntExpression)valueArg).getInt());
                        }
                        break;
                    case "BIGINT_BE":
                    case "BIGINT":
                    case "UINT8_BE":
                    case "UINT8":
                        if (valueArg instanceof ValueExpressions.LongExpression
                                && (isEqualityFn || encodingType.startsWith("U"))) {
                            bb = newByteBuf(8, encodingType.endsWith("_BE"));
                            bb.writeLong(((ValueExpressions.LongExpression)valueArg).getLong());
                        }
                        break;
                    case "FLOAT":
                        if (valueArg instanceof ValueExpressions.FloatExpression && isEqualityFn) {
                            bb = newByteBuf(4, true);
                            bb.writeFloat(((ValueExpressions.FloatExpression)valueArg).getFloat());
                        }
                        break;
                    case "DOUBLE":
                        if (valueArg instanceof ValueExpressions.DoubleExpression && isEqualityFn) {
                            bb = newByteBuf(8, true);
                            bb.writeDouble(((ValueExpressions.DoubleExpression)valueArg).getDouble());
                        }
                        break;
                    case "TIME_EPOCH":
                    case "TIME_EPOCH_BE":
                        if (valueArg instanceof ValueExpressions.TimeExpression) {
                            bb = newByteBuf(8, encodingType.endsWith("_BE"));
                            bb.writeLong(((ValueExpressions.TimeExpression)valueArg).getTime());
                        }
                        break;
                    case "DATE_EPOCH":
                    case "DATE_EPOCH_BE":
                        if (valueArg instanceof ValueExpressions.DateExpression) {
                            bb = newByteBuf(8, encodingType.endsWith("_BE"));
                            bb.writeLong(((ValueExpressions.DateExpression)valueArg).getDate());
                        }
                        break;
                    case "BOOLEAN_BYTE":
                        if (valueArg instanceof ValueExpressions.BooleanExpression) {
                            bb = newByteBuf(1, false /* does not matter */);
                            bb.writeByte(((ValueExpressions.BooleanExpression)valueArg).getBoolean() ? 1 : 0);
                        }
                        break;
                    case "DOUBLE_OB":
                    case "DOUBLE_OBD":
                        throw new RuntimeException("FIXME: I don't know how to handle this data type");
                    case "FLOAT_OB":
                    case "FLOAT_OBD":
                        throw new RuntimeException("FIXME: I don't know how to handle this data type");
                    case "BIGINT_OB":
                    case "BIGINT_OBD":
                        throw new RuntimeException("FIXME: I don't know how to handle this data type");
                    case "INT_OB":
                    case "INT_OBD":
                        throw new RuntimeException("FIXME: I don't know how to handle this data type");
                    case "UTF8":
                        // let visitSchemaPath() handle this.
                        return e.getInput().accept(this, valueArg);
                }

                if (bb != null) {
                    this.value = bb.array();
                    this.path = (SchemaPath)e.getInput();
                    return true;
                }
            }
        }
        return false;
    }


    @Override
    public Boolean visitUnknown(LogicalExpression e, LogicalExpression valueArg) throws RuntimeException {
        return false;
    }

    @Override
    public Boolean visitSchemaPath(SchemaPath path, LogicalExpression valueArg) throws RuntimeException {
        if (valueArg instanceof ValueExpressions.QuotedString) {
            this.value = ((ValueExpressions.QuotedString) valueArg).value.getBytes(Charsets.UTF_8);
            this.path = path;
            return true;
        } else if (valueArg instanceof ValueExpressions.LongExpression) {
            this.value = Bytes.fromLong(((ValueExpressions.LongExpression) valueArg).getLong());
            this.originalValue = ((ValueExpressions.LongExpression) valueArg).getLong();
            this.path = path;
            return true;
        } else if (valueArg instanceof ValueExpressions.IntExpression) {
            this.value = Bytes.fromInt(((ValueExpressions.IntExpression) valueArg).getInt());
            this.originalValue = ((ValueExpressions.IntExpression) valueArg).getInt();
            this.path = path;
            return true;
        }
        return false;
    }

    private static ByteBuf newByteBuf(int size, boolean bigEndian) {
        return Unpooled.wrappedBuffer(new byte[size])
                .order(bigEndian ? ByteOrder.BIG_ENDIAN : ByteOrder.LITTLE_ENDIAN)
                .writerIndex(0);
    }

    private static final ImmutableSet<Class<? extends LogicalExpression>> VALUE_EXPRESSION_CLASSES;
    static {
        ImmutableSet.Builder<Class<? extends LogicalExpression>> builder = ImmutableSet.builder();
        VALUE_EXPRESSION_CLASSES = builder
                .add(ValueExpressions.BooleanExpression.class)
                .add(ValueExpressions.DateExpression.class)
                .add(ValueExpressions.DoubleExpression.class)
                .add(ValueExpressions.FloatExpression.class)
                .add(ValueExpressions.IntExpression.class)
                .add(ValueExpressions.LongExpression.class)
                .add(ValueExpressions.QuotedString.class)
                .add(ValueExpressions.TimeExpression.class)
                .build();
    }

    private static final ImmutableMap<String, String> COMPARE_FUNCTIONS_TRANSPOSE_MAP;
    static {
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        COMPARE_FUNCTIONS_TRANSPOSE_MAP = builder
                // unary functions
                .put("isnotnull", "isnotnull")
                .put("isNotNull", "isNotNull")
                .put("is not null", "is not null")
                .put("isnull", "isnull")
                .put("isNull", "isNull")
                .put("is null", "is null")
                // binary functions
                .put("like", "like")
                .put("equal", "equal")
                .put("not_equal", "not_equal")
                .put("greater_than_or_equal_to", "less_than_or_equal_to")
                .put("greater_than", "less_than")
                .put("less_than_or_equal_to", "greater_than_or_equal_to")
                .put("less_than", "greater_than")
                .build();
    }

}
