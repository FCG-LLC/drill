package org.apache.drill.exec.store.cslogs;

import com.google.common.collect.ImmutableList;
import org.apache.drill.common.expression.BooleanOperator;
import org.apache.drill.common.expression.FunctionCall;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.visitors.AbstractExprVisitor;


public class CSLogsFilterBuilder extends AbstractExprVisitor<CSLogsScanSpec, Void, RuntimeException> {

    final private CSLogsGroupScan groupScan;
//    final private Schema tableSchema;

    final private LogicalExpression le;

    CSLogsFilterBuilder(CSLogsGroupScan groupScan, LogicalExpression le) {
        this.groupScan = groupScan;
//        this.tableSchema = groupScan.getTableSchema();
        this.le = le;
    }

    public CSLogsScanSpec parseTree() {
        CSLogsScanSpec parsedSpec = le.accept(this, null);
//        if (parsedSpec != null) {
//            if (parsedSpec.isPushOr()) {
//                parsedSpec = mergeScanSpecs("booleanOr", this.groupScan.getKuduScanSpec(), parsedSpec);
//            } else {
//                parsedSpec = mergeScanSpecs("booleanAnd", this.groupScan.getKuduScanSpec(), parsedSpec);
//            }
//        }
        return parsedSpec;
    }

    public boolean isAllExpressionsConverted() {
        // That would be a very nice optimization, but it will probably never work right here
        return false;
    }

    @Override
    public CSLogsScanSpec visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
        return null;
    }

    @Override
    public CSLogsScanSpec visitBooleanOperator(BooleanOperator op, Void value) throws RuntimeException {
        return visitFunctionCall(op, value);
    }

    @Override
    public CSLogsScanSpec visitFunctionCall(FunctionCall call, Void value) throws RuntimeException {
        CSLogsScanSpec nodeScanSpec = null;
        String functionName = call.getName();
        ImmutableList<LogicalExpression> args = call.args;

        // FIXME: this could use kudu driver help

        nodeScanSpec = new CSLogsScanSpec();
        return nodeScanSpec;
   }



}