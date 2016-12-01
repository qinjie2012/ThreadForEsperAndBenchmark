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

package com.espertech.esper.epl.subquery;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.collection.MultiKeyUntyped;
import com.espertech.esper.epl.agg.service.AggregationService;
import com.espertech.esper.epl.expression.core.ExprEvaluator;
import com.espertech.esper.epl.expression.core.ExprEvaluatorContext;

import java.util.Collection;

public abstract class SubselectAggregationPreprocessorBase {

    protected final AggregationService aggregationService;
    protected final ExprEvaluator filterExpr;
    protected final ExprEvaluator[] groupKeys;

    public SubselectAggregationPreprocessorBase(AggregationService aggregationService, ExprEvaluator filterExpr, ExprEvaluator[] groupKeys) {
        this.aggregationService = aggregationService;
        this.filterExpr = filterExpr;
        this.groupKeys = groupKeys;
    }

    public abstract void evaluate(EventBean[] eventsPerStream, Collection<EventBean> matchingEvents, ExprEvaluatorContext exprEvaluatorContext);

    protected Object generateGroupKey(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext exprEvaluatorContext) {
        if (groupKeys.length == 1) {
            return groupKeys[0].evaluate(eventsPerStream, isNewData, exprEvaluatorContext);
        }
        Object[] keys = new Object[groupKeys.length];
        for (int i = 0; i < groupKeys.length; i++) {
            keys[i] = groupKeys[i].evaluate(eventsPerStream, isNewData, exprEvaluatorContext);
        }
        return new MultiKeyUntyped(keys);
    }
}
