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
import com.espertech.esper.epl.table.mgmt.TableMetadataColumn;
import com.espertech.esper.epl.expression.core.ExprEvaluator;
import com.espertech.esper.epl.expression.core.ExprEvaluatorContext;
import com.espertech.esper.event.ObjectArrayBackedEventBean;
import com.espertech.esper.event.arr.ObjectArrayEventBean;

import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ExprTableEvalStrategyGroupByTopLevelMulti extends ExprTableEvalStrategyGroupByTopLevelBase {

    private final ExprEvaluator[] groupExpr;

    public ExprTableEvalStrategyGroupByTopLevelMulti(Lock lock, Map<Object, ObjectArrayBackedEventBean> aggregationState, Map<String, TableMetadataColumn> items, ExprEvaluator[] groupExpr) {
        super(lock, aggregationState, items);
        this.groupExpr = groupExpr;
    }

    public Object evaluate(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext exprEvaluatorContext) {
        Object groupKey = ExprTableEvalStrategyGroupByAccessMulti.getKey(groupExpr, eventsPerStream, isNewData, exprEvaluatorContext);
        return super.evaluateInternal(groupKey, eventsPerStream, isNewData, exprEvaluatorContext);
    }

    public Object[] evaluateTypableSingle(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context) {
        Object groupKey = ExprTableEvalStrategyGroupByAccessMulti.getKey(groupExpr, eventsPerStream, isNewData, context);
        return super.evaluateTypableSingleInternal(groupKey, eventsPerStream, isNewData, context);
    }
}
