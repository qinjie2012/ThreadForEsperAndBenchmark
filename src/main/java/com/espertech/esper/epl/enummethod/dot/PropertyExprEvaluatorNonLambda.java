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

package com.espertech.esper.epl.enummethod.dot;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventPropertyGetter;
import com.espertech.esper.epl.expression.core.ExprEvaluator;
import com.espertech.esper.epl.expression.core.ExprEvaluatorContext;

public class PropertyExprEvaluatorNonLambda implements ExprEvaluator {

    private final int streamId;
    private final EventPropertyGetter getter;
    private final Class returnType;

    public PropertyExprEvaluatorNonLambda(int streamId, EventPropertyGetter getter, Class returnType) {
        this.streamId = streamId;
        this.getter = getter;
        this.returnType = returnType;
    }

    public Object evaluate(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context) {
        EventBean eventInQuestion = eventsPerStream[streamId];
        if (eventInQuestion == null) {
            return null;
        }
        return getter.get(eventInQuestion);
    }

    public Class getType() {
        return returnType;
    }

}
