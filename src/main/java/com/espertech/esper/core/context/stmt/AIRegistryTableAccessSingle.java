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

package com.espertech.esper.core.context.stmt;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.epl.expression.table.ExprTableAccessEvalStrategy;
import com.espertech.esper.epl.expression.core.ExprEvaluatorContext;

import java.util.Collection;

public class AIRegistryTableAccessSingle implements AIRegistryTableAccess, ExprTableAccessEvalStrategy {

    private ExprTableAccessEvalStrategy strategy;

    public void assignService(int num, ExprTableAccessEvalStrategy value) {
        this.strategy = value;
    }

    public void deassignService(int num) {
        this.strategy = null;
    }

    public int getAgentInstanceCount() {
        return strategy == null ? 0 : 1;
    }

    public Object evaluate(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext exprEvaluatorContext) {
        return strategy.evaluate(eventsPerStream, isNewData, exprEvaluatorContext);
    }

    public Object[] evaluateTypableSingle(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context) {
        return strategy.evaluateTypableSingle(eventsPerStream, isNewData, context);
    }

    public Collection<EventBean> evaluateGetROCollectionEvents(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context) {
        return strategy.evaluateGetROCollectionEvents(eventsPerStream, isNewData, context);
    }

    public EventBean evaluateGetEventBean(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context) {
        return strategy.evaluateGetEventBean(eventsPerStream, isNewData, context);
    }

    public Collection evaluateGetROCollectionScalar(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context) {
        return strategy.evaluateGetROCollectionScalar(eventsPerStream, isNewData, context);
    }
}
