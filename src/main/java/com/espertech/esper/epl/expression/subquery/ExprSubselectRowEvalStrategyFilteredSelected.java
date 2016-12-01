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

package com.espertech.esper.epl.expression.subquery;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.epl.expression.core.ExprEvaluatorContext;
import com.espertech.esper.event.EventBeanUtility;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ExprSubselectRowEvalStrategyFilteredSelected implements ExprSubselectRowEvalStrategy {

    // Filter and Select
    public Object evaluate(EventBean[] eventsPerStream, boolean newData, Collection<EventBean> matchingEvents, ExprEvaluatorContext exprEvaluatorContext, ExprSubselectRowNode parent) {
        EventBean[] eventsZeroBased = EventBeanUtility.allocatePerStreamShift(eventsPerStream);
        EventBean subSelectResult = ExprSubselectRowNodeUtility.evaluateFilterExpectSingleMatch(eventsZeroBased, newData, matchingEvents, exprEvaluatorContext, parent);
        if (subSelectResult == null) {
            return null;
        }

        eventsZeroBased[0] = subSelectResult;
        Object result;
        if (parent.selectClauseEvaluator.length == 1) {
            result = parent.selectClauseEvaluator[0].evaluate(eventsZeroBased, true, exprEvaluatorContext);
        }
        else {
            // we are returning a Map here, not object-array, preferring the self-describing structure
            result = parent.evaluateRow(eventsZeroBased, true, exprEvaluatorContext);
        }

        return result;
    }

    // Filter and Select
    public Collection<EventBean> evaluateGetCollEvents(EventBean[] eventsPerStream, boolean newData, Collection<EventBean> matchingEvents, ExprEvaluatorContext context, ExprSubselectRowNode parent) {
        return null;
    }

    // Filter and Select
    public Collection evaluateGetCollScalar(EventBean[] eventsPerStream, boolean isNewData, Collection<EventBean> matchingEvents, ExprEvaluatorContext context, ExprSubselectRowNode parent) {
        List<Object> result = new ArrayList<Object>();
        EventBean[] events = EventBeanUtility.allocatePerStreamShift(eventsPerStream);
        for (EventBean subselectEvent : matchingEvents) {
            events[0] = subselectEvent;
            Boolean pass = (Boolean) parent.filterExpr.evaluate(events, true, context);
            if ((pass != null) && (pass)) {
                result.add(parent.selectClauseEvaluator[0].evaluate(events, isNewData, context));
            }
        }
        return result;
    }

    // Filter and Select
    public Object[] typableEvaluate(EventBean[] eventsPerStream, boolean isNewData, Collection<EventBean> matchingEvents, ExprEvaluatorContext exprEvaluatorContext, ExprSubselectRowNode parent) {
        EventBean[] events = EventBeanUtility.allocatePerStreamShift(eventsPerStream);
        EventBean subSelectResult = ExprSubselectRowNodeUtility.evaluateFilterExpectSingleMatch(events, isNewData, matchingEvents, exprEvaluatorContext, parent);
        if (subSelectResult == null) {
            return null;
        }

        events[0] = subSelectResult;
        Object[] results = new Object[parent.selectClauseEvaluator.length];
        for (int i = 0; i < results.length; i++) {
            results[i] = parent.selectClauseEvaluator[i].evaluate(events, isNewData, exprEvaluatorContext);
        }
        return results;
    }

    public Object[][] typableEvaluateMultirow(EventBean[] eventsPerStream, boolean newData, Collection<EventBean> matchingEvents, ExprEvaluatorContext exprEvaluatorContext, ExprSubselectRowNode parent) {
        Object[][] rows = new Object[matchingEvents.size()][];
        int index = -1;
        EventBean[] events = EventBeanUtility.allocatePerStreamShift(eventsPerStream);
        for (EventBean matchingEvent : matchingEvents) {
            events[0] = matchingEvent;

            Boolean pass = (Boolean) parent.filterExpr.evaluate(events, newData, exprEvaluatorContext);
            if ((pass != null) && (pass)) {
                index++;
                Object[] results = new Object[parent.selectClauseEvaluator.length];
                for (int i = 0; i < results.length; i++) {
                    results[i] = parent.selectClauseEvaluator[i].evaluate(events, newData, exprEvaluatorContext);
                }
                rows[index] = results;
            }
        }
        if (index == rows.length - 1) {
            return rows;
        }
        if (index == -1) {
            return new Object[0][];
        }
        Object[][] rowArray = new Object[index + 1][];
        System.arraycopy(rows, 0, rowArray, 0, rowArray.length);
        return rowArray;
    }

    // Filter and Select
    public EventBean evaluateGetEventBean(EventBean[] eventsPerStream, boolean isNewData, Collection<EventBean> matchingEvents, ExprEvaluatorContext context, ExprSubselectRowNode parent) {
        EventBean[] events = EventBeanUtility.allocatePerStreamShift(eventsPerStream);
        EventBean subSelectResult = ExprSubselectRowNodeUtility.evaluateFilterExpectSingleMatch(events, isNewData, matchingEvents, context, parent);
        if (subSelectResult == null) {
            return null;
        }
        Map<String, Object> row = parent.evaluateRow(events, true, context);
        return parent.subselectMultirowType.getEventAdapterService().adapterForTypedMap(row, parent.subselectMultirowType.getEventType());
    }
}
