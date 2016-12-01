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
import com.espertech.esper.epl.agg.access.AggregationAccessorSlotPair;
import com.espertech.esper.epl.agg.access.AggregationState;
import com.espertech.esper.epl.agg.aggregator.AggregationMethod;
import com.espertech.esper.epl.expression.core.ExprEvaluator;
import com.espertech.esper.epl.expression.core.ExprEvaluatorContext;
import com.espertech.esper.metrics.instrumentation.InstrumentationHelper;

import java.util.Collection;

/**
 * Implementation for handling aggregation without any grouping (no group-by).
 */
public class AggSvcGroupAllMixedAccessImpl extends AggregationServiceBaseUngrouped
{
    private final AggregationAccessorSlotPair[] accessors;
    protected AggregationState[] states;

    public AggSvcGroupAllMixedAccessImpl(ExprEvaluator evaluators[], AggregationMethod aggregators[], AggregationAccessorSlotPair[] accessors, AggregationState[] states, AggregationMethodFactory aggregatorFactories[], AggregationStateFactory[] accessAggregations) {
        super(evaluators, aggregators, aggregatorFactories, accessAggregations);
        this.accessors = accessors;
        this.states = states;
    }

    public void applyEnter(EventBean[] eventsPerStream, Object optionalGroupKeyPerRow, ExprEvaluatorContext exprEvaluatorContext)
    {
        if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().qAggregationUngroupedApplyEnterLeave(true, evaluators.length, accessAggregations.length);}
        for (int i = 0; i < evaluators.length; i++)
        {
            if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().qAggNoAccessEnterLeave(true, i, aggregators[i], aggregatorFactories[i].getAggregationExpression());}
            Object columnResult = evaluators[i].evaluate(eventsPerStream, true, exprEvaluatorContext);
            aggregators[i].enter(columnResult);
            if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().aAggNoAccessEnterLeave(true, i, aggregators[i]);}
        }

        for (int i = 0; i < states.length; i++) {
            if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().qAggAccessEnterLeave(true, i, states[i], accessAggregations[i].getAggregationExpression());}
            states[i].applyEnter(eventsPerStream, exprEvaluatorContext);
            if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().aAggAccessEnterLeave(true, i, states[i]);}
        }

        if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().aAggregationUngroupedApplyEnterLeave(true);}
    }

    public void applyLeave(EventBean[] eventsPerStream, Object optionalGroupKeyPerRow, ExprEvaluatorContext exprEvaluatorContext)
    {
        if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().qAggregationUngroupedApplyEnterLeave(false, evaluators.length, accessAggregations.length);}

        for (int i = 0; i < evaluators.length; i++)
        {
            if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().qAggNoAccessEnterLeave(false, i, aggregators[i], aggregatorFactories[i].getAggregationExpression());}
            Object columnResult = evaluators[i].evaluate(eventsPerStream, false, exprEvaluatorContext);
            aggregators[i].leave(columnResult);
            if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().aAggNoAccessEnterLeave(false, i, aggregators[i]);}
        }

        for (int i = 0; i < states.length; i++) {
            if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().qAggAccessEnterLeave(false, i, states[i], accessAggregations[i].getAggregationExpression());}
            states[i].applyLeave(eventsPerStream, exprEvaluatorContext);
            if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().aAggAccessEnterLeave(false, i, states[i]);}
        }

        if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().aAggregationUngroupedApplyEnterLeave(false);}
    }

    public void setCurrentAccess(Object groupKey, int agentInstanceId, AggregationGroupByRollupLevel rollupLevel)
    {
        // no action needed - this implementation does not group and the current row is the single group
    }

    public Object getValue(int column, int agentInstanceId, EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext exprEvaluatorContext)
    {
        if (column < aggregators.length) {
            return aggregators[column].getValue();
        }
        else {
            AggregationAccessorSlotPair pair = accessors[column - aggregators.length];
            return pair.getAccessor().getValue(states[pair.getSlot()], eventsPerStream, isNewData, exprEvaluatorContext);
        }
    }

    public Collection<EventBean> getCollectionOfEvents(int column, EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context) {
        if (column < aggregators.length) {
            return null;
        }
        else {
            AggregationAccessorSlotPair pair = accessors[column - aggregators.length];
            return pair.getAccessor().getEnumerableEvents(states[pair.getSlot()], eventsPerStream, isNewData, context);
        }
    }

    public Collection<Object> getCollectionScalar(int column, EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context) {
        if (column < aggregators.length) {
            return null;
        }
        else {
            AggregationAccessorSlotPair pair = accessors[column - aggregators.length];
            return pair.getAccessor().getEnumerableScalar(states[pair.getSlot()], eventsPerStream, isNewData, context);
        }
    }

    public EventBean getEventBean(int column, EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context) {
        if (column < aggregators.length) {
            return null;
        }
        else {
            AggregationAccessorSlotPair pair = accessors[column - aggregators.length];
            return pair.getAccessor().getEnumerableEvent(states[pair.getSlot()], eventsPerStream, isNewData, context);
        }
    }

    public void clearResults(ExprEvaluatorContext exprEvaluatorContext)
    {
        for (AggregationState state : states) {
            state.clear();
        }
        for (AggregationMethod aggregator : aggregators)
        {
            aggregator.clear();
        }
    }

    public void setRemovedCallback(AggregationRowRemovedCallback callback) {
        // not applicable
    }

    public void accept(AggregationServiceVisitor visitor) {
        visitor.visitAggregations(1, states, aggregators);
    }

    public void acceptGroupDetail(AggregationServiceVisitorWGroupDetail visitor) {
    }

    public boolean isGrouped() {
        return false;
    }

    public Object getGroupKey(int agentInstanceId) {
        return null;
    }

    public Collection<Object> getGroupKeys(ExprEvaluatorContext exprEvaluatorContext) {
        return null;
    }
}