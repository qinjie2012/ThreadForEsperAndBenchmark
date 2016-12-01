/*
 * *************************************************************************************
 *  Copyright (C) 2006-2015 EsperTech, Inc. All rights reserved.                       *
 *  http://www.espertech.com/esper                                                     *
 *  http://www.espertech.com                                                           *
 *  ---------------------------------------------------------------------------------- *
 *  The software in this package is published under the terms of the GPL license       *
 *  a copy of which has been included with this distribution in the license.txt file.  *
 * *************************************************************************************
 */

package com.espertech.esper.epl.agg.service;

import com.espertech.esper.client.annotation.Hint;
import com.espertech.esper.epl.agg.access.AggregationAccessorSlotPair;
import com.espertech.esper.epl.agg.access.AggregationAgent;
import com.espertech.esper.epl.agg.util.AggregationLocalGroupByPlan;
import com.espertech.esper.epl.expression.core.ExprEvaluator;
import com.espertech.esper.epl.expression.core.ExprNode;
import com.espertech.esper.epl.expression.core.ExprValidationException;
import com.espertech.esper.epl.spec.IntoTableSpec;
import com.espertech.esper.epl.table.mgmt.TableColumnMethodPair;
import com.espertech.esper.epl.table.mgmt.TableMetadata;
import com.espertech.esper.epl.variable.VariableService;

public class AggregationServiceFactoryServiceImpl implements AggregationServiceFactoryService {

    public final static AggregationServiceFactoryService DEFAULT_FACTORY = new AggregationServiceFactoryServiceImpl();

    public AggregationServiceFactory getNullAggregationService() {
        return AggregationServiceNullFactory.AGGREGATION_SERVICE_NULL_FACTORY;
    }

    public AggregationServiceFactory getNoGroupNoAccess(ExprEvaluator[] evaluatorsArr, AggregationMethodFactory[] aggregatorsArr) {
        return new AggSvcGroupAllNoAccessFactory(evaluatorsArr, aggregatorsArr, null);
    }

    public AggregationServiceFactory getNoGroupAccessOnly(AggregationAccessorSlotPair[] pairs, AggregationStateFactory[] accessAggSpecs, boolean join) {
        return new AggSvcGroupAllAccessOnlyFactory(pairs, accessAggSpecs, join);
    }

    public AggregationServiceFactory getNoGroupAccessMixed(ExprEvaluator[] evaluatorsArr, AggregationMethodFactory[] aggregatorsArr, AggregationAccessorSlotPair[] pairs, AggregationStateFactory[] accessAggregations, boolean join) {
        return new AggSvcGroupAllMixedAccessFactory(evaluatorsArr, aggregatorsArr, null, pairs, accessAggregations, join);
    }

    public AggregationServiceFactory getGroupedNoReclaimNoAccess(ExprEvaluator[] evaluatorsArr, AggregationMethodFactory[] aggregatorsArr, Object groupKeyBinding) {
        return new AggSvcGroupByNoAccessFactory(evaluatorsArr, aggregatorsArr, groupKeyBinding);
    }

    public AggregationServiceFactory getGroupNoReclaimAccessOnly(AggregationAccessorSlotPair[] pairs, AggregationStateFactory[] accessAggSpecs, Object groupKeyBinding, boolean join) {
        return new AggSvcGroupByAccessOnlyFactory(pairs, accessAggSpecs, groupKeyBinding, join);
    }

    public AggregationServiceFactory getGroupNoReclaimMixed(ExprEvaluator[] evaluatorsArr, AggregationMethodFactory[] aggregatorsArr, AggregationAccessorSlotPair[] pairs, AggregationStateFactory[] accessAggregations, boolean join, Object groupKeyBinding) {
        return new AggSvcGroupByMixedAccessFactory(evaluatorsArr, aggregatorsArr, groupKeyBinding, pairs, accessAggregations, join);
    }

    public AggregationServiceFactory getGroupReclaimAged(ExprEvaluator[] evaluatorsArr, AggregationMethodFactory[] aggregatorsArr, Hint reclaimGroupAged, Hint reclaimGroupFrequency, VariableService variableService, AggregationAccessorSlotPair[] pairs, AggregationStateFactory[] accessAggregations, boolean join, Object groupKeyBinding, String optionalContextName) throws ExprValidationException{
        return new AggSvcGroupByReclaimAgedFactory(evaluatorsArr, aggregatorsArr, groupKeyBinding, reclaimGroupAged, reclaimGroupFrequency, variableService, pairs, accessAggregations, join, optionalContextName);
    }

    public AggregationServiceFactory getGroupReclaimNoAccess(ExprEvaluator[] evaluatorsArr, AggregationMethodFactory[] aggregatorsArr, AggregationAccessorSlotPair[] pairs, AggregationStateFactory[] accessAggregations, boolean join, Object groupKeyBinding) {
        return new AggSvcGroupByRefcountedNoAccessFactory(evaluatorsArr, aggregatorsArr, groupKeyBinding);
    }

    public AggregationServiceFactory getGroupReclaimMixable(ExprEvaluator[] evaluatorsArr, AggregationMethodFactory[] aggregatorsArr, AggregationAccessorSlotPair[] pairs, AggregationStateFactory[] accessAggregations, boolean join, Object groupKeyBinding) {
        return new AggSvcGroupByRefcountedWAccessFactory(evaluatorsArr, aggregatorsArr, groupKeyBinding, pairs, accessAggregations, join);
    }

    public AggregationServiceFactory getGroupReclaimMixableRollup(ExprEvaluator[] evaluatorsArr, AggregationMethodFactory[] aggregatorsArr, AggregationAccessorSlotPair[] pairs, AggregationStateFactory[] accessAggregations, boolean join, Object groupKeyBinding, AggregationGroupByRollupDesc groupByRollupDesc) {
        return new AggSvcGroupByRefcountedWAccessRollupFactory(evaluatorsArr, aggregatorsArr, groupKeyBinding, pairs, accessAggregations, join, groupByRollupDesc);
    }

    public AggregationServiceFactory getGroupWBinding(TableMetadata tableMetadata, TableColumnMethodPair[] methodPairs, AggregationAccessorSlotPair[] accessorPairs, boolean join, IntoTableSpec bindings, int[] targetStates, ExprNode[] accessStateExpr, AggregationAgent[] agents, AggregationGroupByRollupDesc groupByRollupDesc) {
        return new AggSvcGroupByWTableFactory(tableMetadata, methodPairs, accessorPairs, join, targetStates, accessStateExpr, agents, groupByRollupDesc);
    }

    public AggregationServiceFactory getNoGroupWBinding(AggregationAccessorSlotPair[] accessors, boolean join, TableColumnMethodPair[] methodPairs, String tableName, int[] targetStates, ExprNode[] accessStateExpr, AggregationAgent[] agents) {
        return new AggSvcGroupAllMixedAccessWTableFactory(accessors, join, methodPairs, tableName, targetStates, accessStateExpr, agents);
    }

    public AggregationServiceFactory getNoGroupLocalGroupBy(boolean join, AggregationLocalGroupByPlan localGroupByPlan, Object groupKeyBinding) {
        return new AggSvcGroupAllLocalGroupByFactory(join, localGroupByPlan, groupKeyBinding);
    }

    public AggregationServiceFactory getGroupLocalGroupBy(boolean join, AggregationLocalGroupByPlan localGroupByPlan, Object groupKeyBinding) {
        return new AggSvcGroupByLocalGroupByFactory(join, localGroupByPlan, groupKeyBinding);
    }
}
