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

package com.espertech.esper.epl.core.eval;

import com.espertech.esper.client.EventType;
import com.espertech.esper.epl.expression.core.ExprEvaluator;
import com.espertech.esper.event.EventAdapterService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class EvalBase {

    private static final Log log = LogFactory.getLog(EvalBase.class);

    protected final SelectExprContext selectExprContext;
    protected final EventType resultEventType;

    public EvalBase(SelectExprContext selectExprContext, EventType resultEventType) {
        this.selectExprContext = selectExprContext;
        this.resultEventType = resultEventType;
    }

    public EventAdapterService getEventAdapterService() {
        return selectExprContext.getEventAdapterService();
    }

    public EventType getResultEventType() {
        return resultEventType;
    }

    public ExprEvaluator[] getExprNodes() {
        return selectExprContext.getExpressionNodes();
    }
}