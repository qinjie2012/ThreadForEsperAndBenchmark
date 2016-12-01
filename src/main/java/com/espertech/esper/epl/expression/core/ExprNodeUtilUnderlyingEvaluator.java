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

package com.espertech.esper.epl.expression.core;

import com.espertech.esper.client.EventBean;

public class ExprNodeUtilUnderlyingEvaluator implements ExprEvaluator {
    private final int streamNum;
    private final Class resultType;

    public ExprNodeUtilUnderlyingEvaluator(int streamNum, Class resultType) {
        this.streamNum = streamNum;
        this.resultType = resultType;
    }

    public Object evaluate(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context)
    {
        if ((eventsPerStream == null) || (eventsPerStream[streamNum] == null)) {
            return null;
        }
        return eventsPerStream[streamNum].getUnderlying();
    }

    public Class getType()
    {
        return resultType;
    }
}
