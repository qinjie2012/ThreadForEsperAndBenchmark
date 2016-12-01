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

package com.espertech.esper.epl.enummethod.eval;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.epl.expression.core.ExprEvaluator;
import com.espertech.esper.epl.expression.core.ExprEvaluatorContext;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

public class EnumEvalTakeLast implements EnumEval {

    private ExprEvaluator sizeEval;
    private int numStreams;

    public EnumEvalTakeLast(ExprEvaluator sizeEval, int numStreams) {
        this.sizeEval = sizeEval;
        this.numStreams = numStreams;
    }

    public int getStreamNumSize() {
        return numStreams;
    }

    public Object evaluateEnumMethod(EventBean[] eventsLambda, Collection target, boolean isNewData, ExprEvaluatorContext context) {

        Object sizeObj = sizeEval.evaluate(eventsLambda, isNewData, context);
        if (sizeObj == null) {
            return null;
        }

        if (target.isEmpty()) {
            return target;
        }

        int size = ((Number) sizeObj).intValue();
        if (size <= 0) {
            return Collections.emptyList();
        }

        if (target.size() < size) {
            return target;
        }

        if (size == 1) {
            Object last = null;
            for (Object next : target) {
                last = next;
            }
            return Collections.singletonList(last);
        }

        ArrayList<Object> result = new ArrayList<Object>();
        for (Object next : target) {
            result.add(next);
            if (result.size() > size) {
                result.remove(0);
            }
        }
        return result;
    }
}
