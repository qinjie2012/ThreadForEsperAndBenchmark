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

package com.espertech.esper.epl.expression.dot.inner;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventType;
import com.espertech.esper.epl.expression.core.ExprEvaluator;
import com.espertech.esper.epl.expression.core.ExprEvaluatorContext;
import com.espertech.esper.epl.expression.dot.ExprDotEvalRootChildInnerEval;
import com.espertech.esper.epl.rettype.EPType;
import com.espertech.esper.epl.rettype.EPTypeHelper;

import java.lang.reflect.Array;
import java.util.*;

public class InnerEvaluatorArrPrimitiveToColl implements ExprDotEvalRootChildInnerEval {

    private final ExprEvaluator rootEvaluator;

    public InnerEvaluatorArrPrimitiveToColl(ExprEvaluator rootEvaluator) {
        this.rootEvaluator = rootEvaluator;
    }

    public Object evaluate(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext exprEvaluatorContext) {
        Object array = rootEvaluator.evaluate(eventsPerStream, isNewData, exprEvaluatorContext);
        if (array == null) {
            return null;
        }
        int len = Array.getLength(array);
        if (len == 0) {
            return Collections.emptyList();
        }
        if (len == 1) {
            return Collections.singletonList(Array.get(array, 0));
        }
        Deque dq = new ArrayDeque(len);
        for (int i = 0; i < len; i++) {
            dq.add(Array.get(array, i));
        }
        return dq;
    }

    public Collection<EventBean> evaluateGetROCollectionEvents(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context) {
        return null;
    }

    public Collection evaluateGetROCollectionScalar(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context) {
        return null;
    }

    public EventType getEventTypeCollection() {
        return null;
    }

    public Class getComponentTypeCollection() {
        return null;
    }

    public EventBean evaluateGetEventBean(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context) {
        return null;
    }

    public EventType getEventTypeSingle() {
        return null;
    }

    public EPType getTypeInfo() {
        return EPTypeHelper.collectionOfSingleValue(rootEvaluator.getType().getComponentType());
    }
}
