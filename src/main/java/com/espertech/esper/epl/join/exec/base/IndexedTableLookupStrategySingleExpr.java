/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.join.exec.base;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.epl.expression.core.ExprEvaluator;
import com.espertech.esper.epl.expression.core.ExprEvaluatorContext;
import com.espertech.esper.epl.expression.core.ExprNode;
import com.espertech.esper.epl.join.rep.Cursor;
import com.espertech.esper.epl.join.table.PropertyIndexedEventTableSingle;
import com.espertech.esper.epl.lookup.LookupStrategyDesc;
import com.espertech.esper.metrics.instrumentation.InstrumentationHelper;

import java.util.Set;

public class IndexedTableLookupStrategySingleExpr implements JoinExecTableLookupStrategy
{
    private final PropertyIndexedEventTableSingle index;
    private final ExprEvaluator exprEvaluator;
    private final int streamNum;
    private final EventBean[] eventsPerStream;
    private final LookupStrategyDesc strategyDesc;

    /**
     * Ctor.
     * @param index - index to look up in
     */
    public IndexedTableLookupStrategySingleExpr(ExprNode exprNode, int streamNum, PropertyIndexedEventTableSingle index, LookupStrategyDesc strategyDesc)
    {
        if (index == null) {
            throw new IllegalArgumentException("Unexpected null index received");
        }
        this.index = index;
        this.streamNum = streamNum;
        this.strategyDesc = strategyDesc;
        this.eventsPerStream = new EventBean[streamNum + 1];
        exprEvaluator = exprNode.getExprEvaluator();
    }

    /**
     * Returns index to look up in.
     * @return index to use
     */
    public PropertyIndexedEventTableSingle getIndex()
    {
        return index;
    }

    public Set<EventBean> lookup(EventBean theEvent, Cursor cursor, ExprEvaluatorContext exprEvaluatorContext)
    {
        if (InstrumentationHelper.ENABLED) {InstrumentationHelper.get().qIndexJoinLookup(this, index);}

        eventsPerStream[streamNum] = theEvent;
        Object key = exprEvaluator.evaluate(eventsPerStream, true, exprEvaluatorContext);

        if (InstrumentationHelper.ENABLED) {
            Set<EventBean> result = index.lookup(key);
            InstrumentationHelper.get().aIndexJoinLookup(result, key);
            return result;
        }
        return index.lookup(key);
    }

    public String toString()
    {
        return "IndexedTableLookupStrategySingleExpr evaluation" +
                " index=(" + index + ')';
    }

    public LookupStrategyDesc getStrategyDesc() {
        return strategyDesc;
    }
}
