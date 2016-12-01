/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.core;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.epl.agg.service.AggregationService;
import com.espertech.esper.epl.expression.core.ExprEvaluatorContext;
import com.espertech.esper.metrics.instrumentation.InstrumentationHelper;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator for group-by with aggregation.
 */
public class ResultSetAggregateGroupedIterator implements Iterator<EventBean>
{
    private final Iterator<EventBean> sourceIterator;
    private final ResultSetProcessorAggregateGrouped resultSetProcessor;
    private final AggregationService aggregationService;
    private EventBean nextResult;
    private final EventBean[] eventsPerStream;
    private final ExprEvaluatorContext exprEvaluatorContext;

    /**
     * Ctor.
     * @param sourceIterator is the parent iterator
     * @param resultSetProcessor for constructing result rows
     * @param aggregationService for pointing to the right aggregation row
     * @param exprEvaluatorContext context for expression evalauation
     */
    public ResultSetAggregateGroupedIterator(Iterator<EventBean> sourceIterator, ResultSetProcessorAggregateGrouped resultSetProcessor, AggregationService aggregationService, ExprEvaluatorContext exprEvaluatorContext)
    {
        this.sourceIterator = sourceIterator;
        this.resultSetProcessor = resultSetProcessor;
        this.aggregationService = aggregationService;
        eventsPerStream = new EventBean[1];
        this.exprEvaluatorContext = exprEvaluatorContext;
    }

    public boolean hasNext()
    {
        if (nextResult != null)
        {
            return true;
        }
        findNext();
        if (nextResult != null)
        {
            return true;
        }
        return false;
    }

    public EventBean next()
    {
        if (nextResult != null)
        {
            EventBean result = nextResult;
            nextResult = null;
            return result;
        }
        findNext();
        if (nextResult != null)
        {
            EventBean result = nextResult;
            nextResult = null;
            return result;
        }
        throw new NoSuchElementException();
    }

    private void findNext()
    {
        while (sourceIterator.hasNext())
        {
            EventBean candidate = sourceIterator.next();
            eventsPerStream[0] = candidate;

            Object groupKey = resultSetProcessor.generateGroupKey(eventsPerStream, true);
            aggregationService.setCurrentAccess(groupKey, exprEvaluatorContext.getAgentInstanceId(), null);

            Boolean pass = true;
            if (resultSetProcessor.getOptionalHavingNode() != null)
            {
                if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().qHavingClauseJoin(eventsPerStream);}
                pass = (Boolean) resultSetProcessor.getOptionalHavingNode().evaluate(eventsPerStream, true, exprEvaluatorContext);
                if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().aHavingClauseJoin(pass);}
            }
            if (!pass)
            {
                continue;
            }

            nextResult = resultSetProcessor.getSelectExprProcessor().process(eventsPerStream, true, true, exprEvaluatorContext);

            break;
        }
    }

    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
