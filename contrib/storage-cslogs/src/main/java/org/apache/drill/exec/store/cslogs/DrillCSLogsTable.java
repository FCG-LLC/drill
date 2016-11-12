package org.apache.drill.exec.store.cslogs;

import com.google.common.collect.Lists;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.drill.exec.planner.logical.DynamicDrillTable;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;

import java.util.List;

public class DrillCSLogsTable extends DynamicDrillTable {
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(DrillCSLogsTable.class);

    private CSLogsScanSpec emptyScanSpec;

    public DrillCSLogsTable(String storageEngineName, CSLogsStoragePlugin plugin, CSLogsScanSpec emptyScanSpec) {
        super(plugin, storageEngineName, emptyScanSpec);
        this.emptyScanSpec = emptyScanSpec;
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        List<String> names = Lists.newArrayList();
        List<RelDataType> types = Lists.newArrayList();

        // We must list *ALL* possible columns here unfortunately, which might get bit complex-ish

        names.add("time_stamp_bucket");
        types.add(typeFactory.createTypeWithNullability(getSqlTypeFromKuduType(typeFactory, Type.INT32), false));

        names.add("uuid");
        types.add(typeFactory.createTypeWithNullability(getSqlTypeFromKuduType(typeFactory, Type.INT32), false));

        names.add("pattern_id");
        types.add(typeFactory.createTypeWithNullability(getSqlTypeFromKuduType(typeFactory, Type.INT32), false));

        for (int i = 0; i < 1000; i++) {
            String typeName = "param"+i;

            names.add(typeName);
            types.add(typeFactory.createTypeWithNullability(getSqlTypeFromKuduType(typeFactory, Type.STRING), true));
        }

        return typeFactory.createStructType(types, names);
    }

    private RelDataType getSqlTypeFromKuduType(RelDataTypeFactory typeFactory, Type type) {
        switch (type) {
            case BINARY:
                return typeFactory.createSqlType(SqlTypeName.VARBINARY, Integer.MAX_VALUE);
            case BOOL:
                return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
            case DOUBLE:
                return typeFactory.createSqlType(SqlTypeName.DOUBLE);
            case FLOAT:
                return typeFactory.createSqlType(SqlTypeName.FLOAT);
            case INT16:
            case INT32:
            case INT8:
                return typeFactory.createSqlType(SqlTypeName.INTEGER);
            case INT64:
                return typeFactory.createSqlType(SqlTypeName.BIGINT);
            case STRING:
                return typeFactory.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE);
            case UNIXTIME_MICROS:
                return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
            default:
                throw new UnsupportedOperationException("Unsupported type.");
        }
    }
}
