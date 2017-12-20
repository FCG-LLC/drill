/*
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

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.drill.common.exceptions.ExecutionSetupException;
import org.apache.drill.common.exceptions.UserException;
import org.apache.drill.common.expression.SchemaPath;
import org.apache.drill.common.types.TypeProtos.MajorType;
import org.apache.drill.common.types.TypeProtos.MinorType;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.expr.TypeHelper;
import org.apache.drill.exec.ops.FragmentContext;
import org.apache.drill.exec.ops.OperatorContext;
import org.apache.drill.exec.physical.impl.OutputMutator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.store.AbstractRecordReader;
import org.apache.drill.exec.store.kudu.KuduSubScan.KuduSubScanSpec;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.Float4Vector;
import org.apache.drill.exec.vector.Float8Vector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.NullableBigIntVector;
import org.apache.drill.exec.vector.NullableBitVector;
import org.apache.drill.exec.vector.NullableFloat4Vector;
import org.apache.drill.exec.vector.NullableFloat8Vector;
import org.apache.drill.exec.vector.NullableIntVector;
import org.apache.drill.exec.vector.NullableTimeStampVector;
import org.apache.drill.exec.vector.NullableVarBinaryVector;
import org.apache.drill.exec.vector.NullableVarCharVector;
import org.apache.drill.exec.vector.TimeStampVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarBinaryVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduScanner;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.RowResult;
import org.apache.kudu.client.RowResultIterator;

import com.google.common.collect.ImmutableList;

public class KuduRecordReader extends AbstractRecordReader {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(KuduRecordReader.class);

  private final KuduClient client;
  private final KuduSubScanSpec scanSpec;
  private final KuduStoragePluginConfig pluginConfig;

  private final boolean allUnsignedINT8;
  private final boolean allUnsignedINT16;

  private String tableName;

  private KuduScanner scanner;
  private RowResultIterator iterator;

  private OutputMutator output;
  private OperatorContext context;

  public final static int MAXIMUM_ROWS_SUPPORTED_IN_BATCH = 20000;

  private static class ProjectedColumnInfo {
    int index;
    ValueVector vv;
    ColumnSchema kuduColumn;
  }

  private ImmutableList<ProjectedColumnInfo> projectedCols;

  public KuduRecordReader(KuduClient client, KuduSubScan.KuduSubScanSpec subScanSpec, KuduStoragePluginConfig pluginConfig,
      List<SchemaPath> projectedColumns, FragmentContext context) {
    this.client = client;
    this.pluginConfig = pluginConfig;
    this.scanSpec = subScanSpec;

    if (this.pluginConfig != null) {
      this.allUnsignedINT8 = this.pluginConfig.isAllUnsignedINT8();
      this.allUnsignedINT16 = this.pluginConfig.isAllUnsignedINT16();
    } else {
      this.allUnsignedINT8 = false;
      this.allUnsignedINT16 = false;
    }

    setColumns(projectedColumns);
  }

  public static KuduRecordReader buildNoDataReader(KuduClient client, String tableName, List<SchemaPath> projectedColumns, FragmentContext context) {
    KuduRecordReader krr = new KuduRecordReader(client, null, null, projectedColumns, context);
    krr.tableName = tableName;
    return krr;
  }

  @Override
  public void setup(OperatorContext context, OutputMutator output) throws ExecutionSetupException {
    this.output = output;
    this.context = context;
    try {
      KuduTable table = client.openTable(scanSpec.getTableName());

      KuduScanner.KuduScannerBuilder builder = client.newScannerBuilder(table);
      if (!isStarQuery()) {
        List<String> colNames = Lists.newArrayList();
        for (SchemaPath p : this.getColumns()) {
          colNames.add(p.getRootSegmentPath());
        }
        builder.setProjectedColumnNames(colNames);
      }

      context.getStats().startWait();

      try {
        if (scanSpec != null) {
          scanner = scanSpec.deserializeIntoScanner(client);
        }
      } finally {
        context.getStats().stopWait();
      }

    } catch (Exception e) {
      throw new ExecutionSetupException(e);
    }
  }

  static final Map<Type, MinorType> TYPES;

  static {
    TYPES = ImmutableMap.<Type, MinorType> builder()
        .put(Type.BINARY, MinorType.VARBINARY)
        .put(Type.BOOL, MinorType.BIT)
        .put(Type.DOUBLE, MinorType.FLOAT8)
        .put(Type.FLOAT, MinorType.FLOAT4)
        .put(Type.INT8, MinorType.INT)
        .put(Type.INT16, MinorType.INT)
        .put(Type.INT32, MinorType.INT)
        .put(Type.INT64, MinorType.BIGINT)
        .put(Type.STRING, MinorType.VARCHAR)
        .put(Type.UNIXTIME_MICROS, MinorType.TIMESTAMP)
        .build();
  }

  @Override
  // This gets next portion of rows.
  // Note that scanner and iterator are managed outside of this function - it handles MAXIMUM_ROWS_SUPPORTED_IN_BATCH with each call at most
  public int next() {
    int rowCount = 0;

    if (scanner == null) {
      return rowCount;
    }

    if (projectedCols == null) {
      // First iteration? initCols (called by addRowResult) will handle this.
    }  else {
      // Cleanup target vectors
      for (ProjectedColumnInfo pci : projectedCols) {
        pci.vv.clear();;
        pci.vv.allocateNewSafe();
      }
    }

    try {
        context.getStats().startWait();
        if ((iterator != null && iterator.hasNext()) || scanner.hasMoreRows()) {

          try {
            if (iterator == null || !iterator.hasNext()) {
              iterator = scanner.nextRows();
            }

            context.getStats().stopWait();

            context.getStats().startProcessing();
            for (; iterator.hasNext() && rowCount < MAXIMUM_ROWS_SUPPORTED_IN_BATCH; rowCount++) {
              addRowResult(iterator.next(), rowCount);
            }
          } finally {
            context.getStats().stopProcessing();
          }
        }
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    } finally {
      context.getStats().stopWait();
    }

    if (projectedCols == null) {
      // That's not great, but...
      if (rowCount > 0) {
        // only then this is nasty and should not really happen
        throw new RuntimeException("No projected cols available but there are " + rowCount + " rows that should have been stored...");
      }
    } else {
      for (ProjectedColumnInfo pci : projectedCols) {
        pci.vv.getMutator().setValueCount(rowCount);
      }
    }

    return rowCount;
  }

  private void initCols(Schema schema) throws SchemaChangeException {
    ImmutableList.Builder<ProjectedColumnInfo> pciBuilder = ImmutableList.builder();

    for (int i = 0; i < schema.getColumnCount(); i++) {
      ColumnSchema col = schema.getColumnByIndex(i);

      final String name = col.getName();
      final Type kuduType = col.getType();
      MinorType minorType = TYPES.get(kuduType);
      if (minorType == null) {
        logger.warn("Ignoring column that is unsupported.", UserException
            .unsupportedError()
            .message(
                "A column you queried has a data type that is not currently supported by the Kudu storage plugin. "
                    + "The column's name was %s and its Kudu data type was %s. ",
                name, kuduType.toString())
            .addContext("column Name", name)
            .addContext("plugin", "kudu")
            .build(logger));

        continue;
      }
      MajorType majorType;
      if (col.isNullable()) {
        majorType = Types.optional(minorType);
      } else {
        majorType = Types.required(minorType);
      }
      MaterializedField field = MaterializedField.create(name, majorType);
      final Class<? extends ValueVector> clazz = TypeHelper.getValueVectorClass(
          minorType, majorType.getMode());
      ValueVector vector = output.addField(field, clazz);
      vector.setInitialCapacity(MAXIMUM_ROWS_SUPPORTED_IN_BATCH);
      vector.allocateNew();

      ProjectedColumnInfo pci = new ProjectedColumnInfo();
      pci.vv = vector;
      pci.kuduColumn = col;
      pci.index = i;
      pciBuilder.add(pci);
    }

    projectedCols = pciBuilder.build();
  }

  private int transformINT16(short in) {
    if (allUnsignedINT16) {
      return Short.toUnsignedInt(in);
    } else {
      return in;
    }
  }

  private int transformINT8(byte in) {
    if (allUnsignedINT8) {
      return Byte.toUnsignedInt(in);
    } else {
      return in;
    }
  }

  private void addRowResult(RowResult result, int rowIndex) throws SchemaChangeException {
    if (projectedCols == null) {
      // We define the columns with the first known row
      initCols(result.getColumnProjection());
    }

    for (ProjectedColumnInfo pci : projectedCols) {
      switch (pci.kuduColumn.getType()) {
      case BINARY: {
        ByteBuffer value = result.getBinary(pci.index);
        if (pci.kuduColumn.isNullable()) {
          if (result.isNull(pci.index)) {
            ((NullableVarBinaryVector.Mutator) pci.vv.getMutator())
                    .setNull(rowIndex);
          } else {
            ((NullableVarBinaryVector.Mutator) pci.vv.getMutator())
                    .setSafe(rowIndex, value, 0, value.remaining());
          }
        } else {
          ((VarBinaryVector.Mutator) pci.vv.getMutator())
              .setSafe(rowIndex, value, 0, value.remaining());
        }
        break;
      }
      case STRING: {
        if (pci.kuduColumn.isNullable()) {
          if (result.isNull(pci.index)) {
            ((NullableVarCharVector.Mutator) pci.vv.getMutator()).setNull(pci.index);
          } else {
            byte[] strBytes = result.getString(pci.index).getBytes();
            ((NullableVarCharVector.Mutator) pci.vv.getMutator()).setSafe(rowIndex, strBytes, 0, strBytes.length);
          }
        } else {
          ((VarCharVector.Mutator) pci.vv.getMutator()).setSafe(rowIndex, result.getString(pci.index).getBytes());
        }
        break;
      }
      case BOOL:
        if (pci.kuduColumn.isNullable()) {
          if (result.isNull(pci.index)) {
            ((NullableBitVector.Mutator) pci.vv.getMutator())
                    .setNull(rowIndex);
          } else {
            ((NullableBitVector.Mutator) pci.vv.getMutator())
                    .setSafe(rowIndex, result.getBoolean(pci.index) ? 1 : 0);
          }
        } else {
          ((BitVector.Mutator) pci.vv.getMutator())
              .setSafe(rowIndex, result.getBoolean(pci.index) ? 1 : 0);
        }
        break;
      case DOUBLE:
        if (pci.kuduColumn.isNullable()) {
          if (result.isNull(pci.index)) {
            ((NullableFloat8Vector.Mutator) pci.vv.getMutator())
                    .setNull(rowIndex);
          } else {
            ((NullableFloat8Vector.Mutator) pci.vv.getMutator())
                    .setSafe(rowIndex, result.getDouble(pci.index));
          }
        } else {
          ((Float8Vector.Mutator) pci.vv.getMutator())
              .setSafe(rowIndex, result.getDouble(pci.index));
        }
        break;
      case FLOAT:
        if (pci.kuduColumn.isNullable()) {
          if (result.isNull(pci.index)) {
            ((NullableFloat4Vector.Mutator) pci.vv.getMutator())
                    .setNull(rowIndex);
          } else {
            ((NullableFloat4Vector.Mutator) pci.vv.getMutator())
                    .setSafe(rowIndex, result.getFloat(pci.index));
          }
        } else {
          ((Float4Vector.Mutator) pci.vv.getMutator())
              .setSafe(rowIndex, result.getFloat(pci.index));
        }
        break;
      case INT16:
        if (pci.kuduColumn.isNullable()) {
          if (result.isNull(pci.index)) {
            ((NullableIntVector.Mutator) pci.vv.getMutator())
                    .setNull(rowIndex);
          } else {
            ((NullableIntVector.Mutator) pci.vv.getMutator())
                    .setSafe(rowIndex, transformINT16(result.getShort(pci.index)));
          }
        } else {
          ((IntVector.Mutator) pci.vv.getMutator())
                  .setSafe(rowIndex, transformINT16(result.getShort(pci.index)));
        }
        break;
      case INT32:
        if (pci.kuduColumn.isNullable()) {
          if (result.isNull(pci.index)) {
            ((NullableIntVector.Mutator) pci.vv.getMutator())
                    .setNull(rowIndex);
          } else {
            ((NullableIntVector.Mutator) pci.vv.getMutator())
                    .setSafe(rowIndex, result.getInt(pci.index));
          }
        } else {
          ((IntVector.Mutator) pci.vv.getMutator())
              .setSafe(rowIndex, result.getInt(pci.index));
        }
        break;
      case INT8:
        if (pci.kuduColumn.isNullable()) {
          if (result.isNull(pci.index)) {
            ((NullableIntVector.Mutator) pci.vv.getMutator())
                    .setNull(rowIndex);
          } else {
            ((NullableIntVector.Mutator) pci.vv.getMutator())
                    .setSafe(rowIndex, transformINT8(result.getByte(pci.index)));
          }
        } else {
          ((IntVector.Mutator) pci.vv.getMutator())
              .setSafe(rowIndex, transformINT8(result.getByte(pci.index)));
        }
        break;
      case INT64:
        if (pci.kuduColumn.isNullable()) {
          if (result.isNull(pci.index)) {
            ((NullableBigIntVector.Mutator) pci.vv.getMutator())
             .setNull(rowIndex);
          } else {
            ((NullableBigIntVector.Mutator) pci.vv.getMutator())
             .setSafe(rowIndex, result.getLong(pci.index));
          }
        } else {
          ((BigIntVector.Mutator) pci.vv.getMutator())
             .setSafe(rowIndex, result.getLong(pci.index));
        }
        break;
      case UNIXTIME_MICROS:
        if (pci.kuduColumn.isNullable()) {
          if (result.isNull(pci.index)) {
            ((NullableTimeStampVector.Mutator) pci.vv.getMutator())
              .setNull(rowIndex);
          } else {
            ((NullableTimeStampVector.Mutator) pci.vv.getMutator())
              .setSafe(rowIndex, result.getLong(pci.index) / 1000);
          }
        } else {
          ((TimeStampVector.Mutator) pci.vv.getMutator())
              .setSafe(rowIndex, result.getLong(pci.index) / 1000);
        }
        break;
      default:
        throw new SchemaChangeException("unknown type"); // TODO make better
      }
    }
  }

  @Override
  public void close() {
    if (projectedCols == null) {
      try {
        // We must provide any schema or the query will fail. So if no results, lets just get what's available in Kudu schema
        String table = this.scanSpec == null ? tableName : this.scanSpec.getTableName();
        Schema schema = client.openTable(table).getSchema();
        initCols(schema);
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {

      }
    }

    try {
      if (scanner != null) {
        scanner.close();
      }
    } catch (KuduException ex) {
      throw new RuntimeException(ex);
    }
  }

}
