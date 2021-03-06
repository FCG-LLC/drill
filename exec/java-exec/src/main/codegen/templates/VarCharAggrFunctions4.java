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
<@pp.dropOutputFile />



<#list aggrtypes4.aggrtypes as aggrtype>
<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/gaggr/${aggrtype.className}VarBytesFunctions.java" />

<#include "/@includes/license.ftl" />

// Source code generated using FreeMarker template ${.template_name}

<#-- A utility class that is used to generate java code for aggr functions that concatenate a string -->
<#-- to hold the result.  This includes: GROUP_CONCAT. -->

        package org.apache.drill.exec.expr.fn.impl.gaggr;

<#include "/@includes/vv_imports.ftl" />

        import org.apache.drill.exec.expr.DrillAggFunc;
        import org.apache.drill.exec.expr.annotations.FunctionTemplate;
        import org.apache.drill.exec.expr.annotations.FunctionTemplate.FunctionScope;
        import org.apache.drill.exec.expr.annotations.Output;
        import org.apache.drill.exec.expr.annotations.Param;
        import org.apache.drill.exec.expr.annotations.Workspace;
        import org.apache.drill.exec.expr.fn.impl.ByteFunctionHelpers;
        import org.apache.drill.exec.expr.holders.*;
        import javax.inject.Inject;
        import org.apache.drill.exec.record.RecordBatch;

        import io.netty.buffer.ByteBuf;

@SuppressWarnings("unused")

public class ${aggrtype.className}VarBytesFunctions {

static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(${aggrtype.className}VarBytesFunctions.class);

<#list aggrtype.types as type>
<#if type.major == "VarBytes">

@FunctionTemplate(name = "${aggrtype.funcName}", scope = FunctionTemplate.FunctionScope.POINT_AGGREGATE)
public static class ${type.inputType}${aggrtype.className} implements DrillAggFunc{

@Param ${type.inputType}Holder in;
@Workspace ObjectHolder value;
@Workspace UInt1Holder init;
@Inject DrillBuf buf;
@Output ${type.outputType}Holder out;

public void setup() {
        init = new UInt1Holder();
        init.value = 0;
        value = new ObjectHolder();
        org.apache.drill.exec.expr.fn.impl.DrillByteArray tmp = new org.apache.drill.exec.expr.fn.impl.DrillByteArray();
        value.obj = tmp;
        }

@Override
public void add() {
<#if type.inputType?starts_with("Nullable")>
        sout: {
        if (in.isSet == 0) {
        // processing nullable input and the value is null, so don't do anything...
        break sout;
        }
</#if>
        org.apache.drill.exec.expr.fn.impl.DrillByteArray tmp = (org.apache.drill.exec.expr.fn.impl.DrillByteArray) value.obj;
        int cmp = 0;
        boolean swap = false;

        // if buffer is null then just put it
        if (init.value == 0) {
        init.value = 1;
        byte[] tempArray = new byte[in.end - in.start];
        in.buffer.getBytes(in.start, tempArray, 0, in.end - in.start);
        tmp.setBytes(tempArray);
        } else {
        byte[] sep = ",".getBytes();
        byte[] tempArray = new byte[in.end - in.start + tmp.getLength()+sep.length];

        System.arraycopy(tmp.getBytes(), 0, tempArray, 0, tmp.getLength());
        System.arraycopy(sep, 0, tempArray, tmp.getLength(), sep.length);
        in.buffer.getBytes(in.start, tempArray, tmp.getLength()+sep.length, in.end - in.start);

        tmp.setBytes(tempArray);
        }
<#if type.inputType?starts_with("Nullable")>
        } // end of sout block
</#if>
        }

@Override
public void output() {
        out.isSet = 1;
        org.apache.drill.exec.expr.fn.impl.DrillByteArray tmp = (org.apache.drill.exec.expr.fn.impl.DrillByteArray) value.obj;
        buf = buf.reallocIfNeeded(tmp.getLength());
        buf.setBytes(0, tmp.getBytes(), 0, tmp.getLength());
        out.start  = 0;
        out.end    = tmp.getLength();
        out.buffer = buf;
        }

@Override
public void reset() {
        value = new ObjectHolder();
        value.obj = new org.apache.drill.exec.expr.fn.impl.DrillByteArray();
        init.value = 0;
        }
        }
</#if>
</#list>
        }
</#list>
