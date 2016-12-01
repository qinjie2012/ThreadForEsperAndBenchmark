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

package com.espertech.esper.epl.table.strategy;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.epl.expression.core.ExprEvaluatorContext;
import com.espertech.esper.epl.expression.core.ExprEvaluatorEnumerationGivenEvent;
import com.espertech.esper.epl.expression.table.ExprTableAccessEvalStrategy;
import com.espertech.esper.event.ObjectArrayBackedEventBean;
import com.espertech.esper.event.arr.ObjectArrayEventBean;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.locks.Lock;

public abstract class ExprTableEvalStrategyGroupByPropBase extends ExprTableEvalStrategyGroupByBase implements ExprTableAccessEvalStrategy {

    private final int propertyIndex;
    private final ExprEvaluatorEnumerationGivenEvent optionalEnumEval;

    protected ExprTableEvalStrategyGroupByPropBase(Lock lock, Map<Object, ObjectArrayBackedEventBean> aggregationState, int propertyIndex, ExprEvaluatorEnumerationGivenEvent optionalEnumEval) {
        super(lock, aggregationState);
        this.propertyIndex = propertyIndex;
        this.optionalEnumEval = optionalEnumEval;
    }

    public Object evaluateInternal(Object groupKey, ExprEvaluatorContext context) {
        ObjectArrayBackedEventBean row = lockTableReadAndGet(groupKey, context);
        if (row == null) {
            return null;
        }
        return row.getProperties()[propertyIndex];
    }

    public Collection<EventBean> evaluateGetROCollectionEventsInternal(Object groupKey, ExprEvaluatorContext context) {
        ObjectArrayBackedEventBean row = lockTableReadAndGet(groupKey, context);
        if (row == null) {
            return null;
        }
        return optionalEnumEval.evaluateEventGetROCollectionEvents(row, context);
    }

    public EventBean evaluateGetEventBeanInternal(Object groupKey, ExprEvaluatorContext context) {
        ObjectArrayBackedEventBean row = lockTableReadAndGet(groupKey, context);
        if (row == null) {
            return null;
        }
        return optionalEnumEval.evaluateEventGetEventBean(row, context);
    }

    public Collection evaluateGetROCollectionScalarInternal(Object groupKey, ExprEvaluatorContext context) {
        ObjectArrayBackedEventBean row = lockTableReadAndGet(groupKey, context);
        if (row == null) {
            return null;
        }
        return optionalEnumEval.evaluateEventGetROCollectionScalar(row, context);
    }

    public Object[] evaluateTypableSingle(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context) {
        return null;
    }
}
