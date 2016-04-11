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
package org.apache.drill.store.kudu;

import org.apache.drill.exec.store.kudu.KuduFilterBuilder;
import org.apache.drill.test.DrillTest;
import org.junit.Test;
import org.kududb.ColumnSchema;
import org.kududb.Type;
import org.kududb.client.ColumnRangePredicate;


import static org.junit.Assert.assertEquals;

public class TestKuduFilterBuilder extends DrillTest {

    @Test
    public void testColumnBoundSetter() throws Exception {
        forType(Type.INT8, (byte) 13, (byte) 23);
        forType(Type.INT16, (short) 123, (short) 223);
        forType(Type.INT32, 123, 223);
        forType(Type.INT64, 123L, 223L);
    }

    private void forType(Type type, Object v1, Object v2) {
        ColumnSchema.ColumnSchemaBuilder builder = new ColumnSchema.ColumnSchemaBuilder("foo", type);

        KuduFilterBuilder.ColumnTypeBoundSetter setter = new KuduFilterBuilder.ColumnTypeBoundSetter(new ColumnRangePredicate(builder.build()));
        assertEquals(v2, setter.getBigger(v1, v2));
        assertEquals(v1, setter.getSmaller(v1, v2));
    }
}
