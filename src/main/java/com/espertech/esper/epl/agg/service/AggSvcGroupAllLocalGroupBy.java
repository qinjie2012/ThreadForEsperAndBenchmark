/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.agg.service;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.collection.MultiKeyUntyped;
import com.espertech.esper.collection.Pair;
import com.espertech.esper.epl.agg.access.AggregationState;
import com.espertech.esper.epl.agg.aggregator.AggregationMethod;
import com.espertech.esper.epl.agg.util.AggregationLocalGroupByColumn;
import com.espertech.esper.epl.agg.util.AggregationLocalGroupByLevel;
import com.espertech.esper.epl.agg.util.AggregationLocalGroupByPlan;
import com.espertech.esper.epl.core.MethodResolutionService;
import com.espertech.esper.epl.expression.core.ExprEvaluator;
import com.espertech.esper.epl.expression.core.ExprEvaluatorContext;

import java.util.*;

/**
 * Implementation for handling aggregation with grouping by group-keys.
 */
public class AggSvcGroupAllLocalGroupBy extends AggSvcGroupLocalGroupByBase
{
    public AggSvcGroupAllLocalGroupBy(MethodResolutionService methodResolutionService, boolean isJoin, AggregationLocalGroupByPlan localGroupByPlan, Object groupKeyBinding) {
        super(methodResolutionService, isJoin, localGroupByPlan, groupKeyBinding);
    }

    protected Object computeGroupKey(AggregationLocalGroupByLevel level, Object groupKey, ExprEvaluator[] partitionEval, EventBean[] eventsPerStream, boolean newData, ExprEvaluatorContext exprEvaluatorContext) {
        return AggSvcGroupLocalGroupByBase.computeGroupKey(partitionEval, eventsPerStream, newData, exprEvaluatorContext);
    }

    public void setCurrentAccess(Object groupByKey, int agentInstanceId, AggregationGroupByRollupLevel rollupLevel)
    {
    }

    public Object getValue(int column, int agentInstanceId, EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext exprEvaluatorContext)
    {
        AggregationLocalGroupByColumn col = localGroupByPlan.getColumns()[column];

        if (col.getPartitionEvaluators().length == 0) {
            if (col.isMethodAgg()) {
                return aggregatorsTopLevel[col.getMethodOffset()].getValue();
            }
            return col.getPair().getAccessor().getValue(statesTopLevel[col.getPair().getSlot()], eventsPerStream, isNewData, exprEvaluatorContext);
        }

        Object groupByKey = computeGroupKey(col.getPartitionEvaluators(), eventsPerStream, true, exprEvaluatorContext);
        AggregationMethodPairRow row = aggregatorsPerLevelAndGroup[col.getLevelNum()].get(groupByKey);
        if (col.isMethodAgg()) {
            return row.getMethods()[col.getMethodOffset()].getValue();
        }
        return col.getPair().getAccessor().getValue(row.getStates()[col.getPair().getSlot()], eventsPerStream, isNewData, exprEvaluatorContext);
    }
}
