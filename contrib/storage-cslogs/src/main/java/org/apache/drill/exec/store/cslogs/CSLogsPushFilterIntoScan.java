package org.apache.drill.exec.store.cslogs;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.exec.planner.logical.DrillOptiq;
import org.apache.drill.exec.planner.logical.DrillParseContext;
import org.apache.drill.exec.planner.logical.RelOptHelper;
import org.apache.drill.exec.planner.physical.FilterPrel;
import org.apache.drill.exec.planner.physical.PrelUtil;
import org.apache.drill.exec.planner.physical.ProjectPrel;
import org.apache.drill.exec.planner.physical.ScanPrel;
import org.apache.drill.exec.store.StoragePluginOptimizerRule;

public abstract class CSLogsPushFilterIntoScan extends StoragePluginOptimizerRule {

    private CSLogsPushFilterIntoScan(RelOptRuleOperand operand, String description) {
        super(operand, description);
    }

    public static final StoragePluginOptimizerRule FILTER_ON_SCAN = new CSLogsPushFilterIntoScan(RelOptHelper.some(FilterPrel.class, RelOptHelper.any(ScanPrel.class)), "CSLogsPushFilterIntoScan:Filter_On_Scan") {

        @Override
        public void onMatch(RelOptRuleCall call) {
            final ScanPrel scan = (ScanPrel) call.rel(1);
            final FilterPrel filter = (FilterPrel) call.rel(0);
            final RexNode condition = filter.getCondition();

            CSLogsGroupScan groupScan = (CSLogsGroupScan)scan.getGroupScan();
            if (groupScan.isFilterPushedDown()) {
        /*
         * The rule can get triggered again due to the transformed "scan => filter" sequence
         * created by the earlier execution of this rule when we could not do a complete
         * conversion of Optiq Filter's condition to HBase Filter. In such cases, we rely upon
         * this flag to not do a re-processing of the rule on the already transformed call.
         */
                return;
            }

            doPushFilterToScan(call, filter, null, scan, groupScan, condition);
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            final ScanPrel scan = (ScanPrel) call.rel(1);
            if (scan.getGroupScan() instanceof CSLogsGroupScan) {
                return super.matches(call);
            }
            return false;
        }
    };



    public static final StoragePluginOptimizerRule FILTER_ON_PROJECT = new CSLogsPushFilterIntoScan(RelOptHelper.some(FilterPrel.class, RelOptHelper.some(ProjectPrel.class, RelOptHelper.any(ScanPrel.class))), "CSLogsPushFilterIntoScan:Filter_On_Project") {

        @Override
        public void onMatch(RelOptRuleCall call) {
            final ScanPrel scan = (ScanPrel) call.rel(2);
            final ProjectPrel project = (ProjectPrel) call.rel(1);
            final FilterPrel filter = (FilterPrel) call.rel(0);

            CSLogsGroupScan groupScan = (CSLogsGroupScan)scan.getGroupScan();
            if (groupScan.isFilterPushedDown()) {
        /*
         * The rule can get triggered again due to the transformed "scan => filter" sequence
         * created by the earlier execution of this rule when we could not do a complete
         * conversion of Optiq Filter's condition to HBase Filter. In such cases, we rely upon
         * this flag to not do a re-processing of the rule on the already transformed call.
         */
                return;
            }

            // convert the filter to one that references the child of the project
            final RexNode condition =  RelOptUtil.pushFilterPastProject(filter.getCondition(), project);

            doPushFilterToScan(call, filter, project, scan, groupScan, condition);
        }

        @Override
        public boolean matches(RelOptRuleCall call) {
            final ScanPrel scan = (ScanPrel) call.rel(2);
            if (scan.getGroupScan() instanceof CSLogsGroupScan) {
                return super.matches(call);
            }
            return false;
        }
    };
    protected void doPushFilterToScan(final RelOptRuleCall call, final FilterPrel filter, final ProjectPrel project, final ScanPrel scan, final CSLogsGroupScan groupScan, final RexNode condition) {

        final LogicalExpression conditionExp = DrillOptiq.toDrill(new DrillParseContext(PrelUtil.getPlannerSettings(call.getPlanner())), scan, condition);
        final CSLogsFilterBuilder kuduFilterBuilder = new CSLogsFilterBuilder(groupScan, conditionExp);
        final CSLogsScanSpec newScanSpec = kuduFilterBuilder.parseTree();
        if (newScanSpec == null) {
            return; //no filter pushdown ==> No transformation.
        }

        final CSLogsGroupScan newGroupsScan = new CSLogsGroupScan(groupScan.getStoragePlugin(), newScanSpec, groupScan.getColumns());
        newGroupsScan.setFilterPushedDown(true);

        final ScanPrel newScanPrel = ScanPrel.create(scan, filter.getTraitSet(), newGroupsScan, scan.getRowType());

        // Depending on whether is a project in the middle, assign either scan or copy of project to childRel.
        final RelNode childRel = project == null ? newScanPrel : project.copy(project.getTraitSet(), ImmutableList.of((RelNode)newScanPrel));;

        if (kuduFilterBuilder.isAllExpressionsConverted()) {
        /*
         * Since we could convert the entire filter condition expression into an HBase filter,
         * we can eliminate the filter operator altogether.
         */
            call.transformTo(childRel);
        } else {
            call.transformTo(filter.copy(filter.getTraitSet(), ImmutableList.of(childRel)));
        }
    }

}
