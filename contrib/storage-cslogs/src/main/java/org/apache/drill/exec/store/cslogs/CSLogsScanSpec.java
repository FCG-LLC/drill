package org.apache.drill.exec.store.cslogs;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;
import org.apache.kudu.client.KuduPredicate;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kudu.Schema;
import org.apache.kudu.client.ColumnRangePredicate;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanToken;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CSLogsScanSpec {
    public static class LogParamSpec {
        long patternId;
        int paramNo;

        @JsonCreator
        public LogParamSpec(long patternId, int paramNo) {
            this.patternId = patternId;
            this.paramNo = paramNo;
        }

        public int getParamNo() {
            return paramNo;
        }

        public long getPatternId() {
            return patternId;
        }

        @Override
        public String toString() {
            return "#"+getParamNo()+"@" + getPatternId();
        }
    }

    private List<KuduPredicate> paramsInvertedPredicates = new ArrayList<>();
    private List<LogParamSpec> projectedParamSpecs = new ArrayList<>();

    @JsonCreator
    public CSLogsScanSpec() {
    }

    public List<KuduPredicate> getParamsInvertedPredicates() {
        return paramsInvertedPredicates;
    }

    public List<LogParamSpec> getProjectedParamSpecs() { return projectedParamSpecs; }

    protected String toString(boolean simplified) {

        StringBuilder sb = new StringBuilder();
        if (!simplified) {
            sb.append("Predicates on log_params_inverted: ");
        }

        List<String> conditions = new ArrayList<>();
        for (KuduPredicate pred : paramsInvertedPredicates) {
            conditions.add(pred.toString());
        }
        sb.append(StringUtils.join(conditions, " AND "));

        sb.append("Projected params: ");
        List<String> specs = new ArrayList<>();
        for (LogParamSpec spec : projectedParamSpecs) {
            specs.add(spec.toString());
        }

        sb.append(StringUtils.join(specs, ", "));

        return sb.toString();
    }
}