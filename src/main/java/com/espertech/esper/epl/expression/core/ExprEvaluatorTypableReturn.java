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

import java.util.LinkedHashMap;

/**
 * Interface for evaluators that select possible multi-valued results in a single select column,
 * such as subqueries and "new" and case+new combined.
 * <p>
 *     When returning non-null results from {#getRowProperties},
 *     the {@link ExprEvaluator#evaluate(com.espertech.esper.client.EventBean[], boolean, ExprEvaluatorContext)}
 *     must return an instance of Map<String, Object> (HashMap is fine).
 * </p>
 * <p>
 *     When returning non-null results, the
 *     the evaluator must also return either Object[] results or Object[][],
 *     each object-array following the same exact order as provided by the map,
 *     matching the multi-row flag.
 * </p>
 */
public interface ExprEvaluatorTypableReturn extends ExprEvaluator {

    /**
     * Return null to indicate no row-type result available,
     * or a map of property names and types to indicate a row-type result is available.
     * @return map of property names and types or null
     * @throws ExprValidationException if the expression is invalid
     */
    public LinkedHashMap<String, Object> getRowProperties() throws ExprValidationException;

    /**
     * Return true for multi-row return, return false for return of single row only
     * @return multi-row flag
     */
    public Boolean isMultirow();

    public Object[] evaluateTypableSingle(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context);
    public Object[][] evaluateTypableMulti(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context);
}
