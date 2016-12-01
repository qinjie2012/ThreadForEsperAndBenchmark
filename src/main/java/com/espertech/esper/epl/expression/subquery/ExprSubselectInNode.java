/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.expression.subquery;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventType;
import com.espertech.esper.epl.expression.core.ExprEvaluatorContext;
import com.espertech.esper.epl.expression.core.ExprValidationContext;
import com.espertech.esper.epl.expression.core.ExprValidationException;
import com.espertech.esper.epl.spec.StatementSpecRaw;
import com.espertech.esper.event.EventAdapterService;

import java.util.Collection;
import java.util.LinkedHashMap;

/**
 * Represents a subselect in an expression tree.
 */
public class ExprSubselectInNode extends ExprSubselectNode
{
    private final boolean isNotIn;
    private transient SubselectEvalStrategy subselectEvalStrategy;
    private static final long serialVersionUID = -7233906204211162498L;

    /**
     * Ctor.
     * @param statementSpec is the lookup statement spec from the parser, unvalidated
     */
    public ExprSubselectInNode(StatementSpecRaw statementSpec, boolean isNotIn)
    {
        super(statementSpec);
        this.isNotIn = isNotIn;
    }

    public Class getType()
    {
        return Boolean.class;
    }

    /**
     * Returns true for not-in, or false for in.
     * @return true for not-in
     */
    public boolean isNotIn()
    {
        return isNotIn;
    }

    public void validateSubquery(ExprValidationContext validationContext) throws ExprValidationException
    {
        subselectEvalStrategy = SubselectEvalStrategyFactory.createStrategy(this, isNotIn, false, false, null);
    }

    public Object evaluate(EventBean[] eventsPerStream, boolean isNewData, Collection<EventBean> matchingEvents, ExprEvaluatorContext exprEvaluatorContext)
    {
        return subselectEvalStrategy.evaluate(eventsPerStream, isNewData, matchingEvents, exprEvaluatorContext);
    }

    public LinkedHashMap<String, Object> typableGetRowProperties() {
        return null;
    }

    public Object[] evaluateTypableSingle(EventBean[] eventsPerStream, boolean isNewData, Collection<EventBean> matchingEvents, ExprEvaluatorContext exprEvaluatorContext) {
        return null;
    }

    public Object[][] evaluateTypableMulti(EventBean[] eventsPerStream, boolean isNewData, Collection<EventBean> matchingEvents, ExprEvaluatorContext exprEvaluatorContext) {
        return null;
    }

    public Collection<EventBean> evaluateGetCollEvents(EventBean[] eventsPerStream, boolean isNewData, Collection<EventBean> matchingEvents, ExprEvaluatorContext context) {
        return null;
    }

    public EventType getEventTypeCollection(EventAdapterService eventAdapterService, String statementId) {
        return null;
    }

    public Class getComponentTypeCollection() throws ExprValidationException {
        return null;
    }

    public EventBean evaluateGetEventBean(EventBean[] eventsPerStream, boolean isNewData, Collection<EventBean> matchingEvents, ExprEvaluatorContext exprEvaluatorContext) {
        return null;
    }

    public Collection evaluateGetCollScalar(EventBean[] eventsPerStream, boolean isNewData, Collection<EventBean> matchingEvents, ExprEvaluatorContext exprEvaluatorContext) {
        return null;
    }

    public boolean isAllowMultiColumnSelect() {
        return false;
    }

    public EventType getEventTypeSingle(EventAdapterService eventAdapterService, String statementId) throws ExprValidationException {
        return null;
    }

    public EventBean evaluateGetEventBean(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context) {
        return null;
    }
}
