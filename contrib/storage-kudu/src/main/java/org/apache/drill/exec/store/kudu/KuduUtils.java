package org.apache.drill.exec.store.kudu;

import org.apache.kudu.client.KuduPredicate;

import java.util.ArrayList;
import java.util.List;

public class KuduUtils {

    public boolean isNone(KuduPredicate pred) {
        // Unfortunately we cannot check KuduPredicate.type because it's hidden...
        return (pred != null && pred.toString().endsWith("NONE"));
    }

    public KuduPredicate findFirstNone(List<KuduPredicate> preds) {
        for (KuduPredicate pred: preds) {
            if (isNone(pred)) {
                return pred;
            }
        }
        return null;
    }

}
