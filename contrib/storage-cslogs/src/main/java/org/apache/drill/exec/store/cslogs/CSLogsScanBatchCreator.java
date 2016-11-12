package org.apache.drill.exec.store.cslogs;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.physical.base.GroupScan;
import org.apache.drill.exec.physical.impl.BatchCreator;
import org.apache.drill.exec.physical.impl.ScanBatch;
import org.apache.drill.exec.record.RecordBatch;
import org.apache.drill.exec.store.RecordReader;

import java.util.List;

public class CSLogsScanBatchCreator implements BatchCreator<CSLogsSubScan> {
    static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(CSLogsScanBatchCreator.class);

    @Override
    public ScanBatch getBatch(FragmentContext context, CSLogsSubScan subScan, List<RecordBatch> children)
            throws ExecutionSetupException {
        Preconditions.checkArgument(children.isEmpty());
        List<RecordReader> readers = Lists.newArrayList();
        List<SchemaPath> columns = null;

        for (CSLogsSubScan.CSLogsSubScanSpec scanSpec : subScan.getTabletScanSpecList()) {
            try {
                if ((columns = subScan.getColumns())==null) {
                    columns = GroupScan.ALL_COLUMNS;
                }
                readers.add(new CSLogsRecordReader(subScan.getStorageEngine().getClient(), scanSpec, columns, context));
            } catch (Exception e1) {
                throw new ExecutionSetupException(e1);
            }
        }
        return new ScanBatch(subScan, context, readers.iterator());
    }

}
