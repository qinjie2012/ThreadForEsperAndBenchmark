/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.expression.methodagg;

import com.espertech.esper.client.EventType;
import com.espertech.esper.epl.agg.access.AggregationAccessor;
import com.espertech.esper.epl.agg.access.AggregationAgent;
import com.espertech.esper.epl.agg.aggregator.AggregationMethod;
import com.espertech.esper.epl.agg.service.AggregationMethodFactoryUtil;
import com.espertech.esper.epl.agg.service.AggregationStateFactory;
import com.espertech.esper.epl.agg.access.AggregationStateKey;
import com.espertech.esper.epl.agg.service.AggregationMethodFactory;
import com.espertech.esper.epl.core.MethodResolutionService;
import com.espertech.esper.epl.expression.core.ExprEvaluator;
import com.espertech.esper.epl.expression.core.ExprValidationException;
import com.espertech.esper.epl.expression.baseagg.ExprAggregateNodeBase;

public class ExprNthAggNodeFactory implements AggregationMethodFactory
{
    private final ExprNthAggNode parent;
    private final Class childType;
    private int size;

    public ExprNthAggNodeFactory(ExprNthAggNode parent, Class childType, int size)
    {
        this.parent = parent;
        this.childType = childType;
        this.size = size;
    }

    public boolean isAccessAggregation() {
        return false;
    }

    public Class getResultType()
    {
        return childType;
    }

    public AggregationStateKey getAggregationStateKey(boolean isMatchRecognize) {
        throw new IllegalStateException("Not an access aggregation function");
    }

    public AggregationStateFactory getAggregationStateFactory(boolean isMatchRecognize) {
        throw new IllegalStateException("Not an access aggregation function");
    }

    public AggregationAccessor getAccessor() {
        throw new IllegalStateException("Not an access aggregation function");
    }

    public AggregationMethod make(MethodResolutionService methodResolutionService, int agentInstanceId, int groupId, int aggregationId) {
        AggregationMethod method = methodResolutionService.makeNthAggregator(agentInstanceId, groupId, aggregationId, childType, size + 1);
        if (!parent.isDistinct()) {
            return method;
        }
        return methodResolutionService.makeDistinctAggregator(agentInstanceId, groupId, aggregationId, method, childType, false);
    }

    public ExprAggregateNodeBase getAggregationExpression() {
        return parent;
    }

    public void validateIntoTableCompatible(AggregationMethodFactory intoTableAgg) throws ExprValidationException {
        AggregationMethodFactoryUtil.validateAggregationType(this, intoTableAgg);
        ExprNthAggNodeFactory that = (ExprNthAggNodeFactory) intoTableAgg;
        AggregationMethodFactoryUtil.validateAggregationInputType(childType, that.childType);
        if (size != that.size) {
            throw new ExprValidationException("The size is " +
                    size +
                    " and provided is " +
                    that.size);
        }
    }

    public AggregationAgent getAggregationStateAgent() {
        return null;
    }

    public ExprEvaluator getMethodAggregationEvaluator(boolean join, EventType[] typesPerStream) throws ExprValidationException {
        return ExprMethodAggUtil.getDefaultEvaluator(parent.getPositionalParams(), join, typesPerStream);
    }
}