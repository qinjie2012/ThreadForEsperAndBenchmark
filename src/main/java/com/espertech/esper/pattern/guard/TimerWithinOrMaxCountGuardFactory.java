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

package com.espertech.esper.pattern.guard;

import com.espertech.esper.client.EPException;
import com.espertech.esper.epl.expression.core.ExprNode;
import com.espertech.esper.epl.expression.time.ExprTimePeriod;
import com.espertech.esper.pattern.*;
import com.espertech.esper.util.JavaClassHelper;
import com.espertech.esper.util.MetaDefItem;

import java.io.Serializable;
import java.util.List;

public class TimerWithinOrMaxCountGuardFactory implements GuardFactory, MetaDefItem, Serializable
{
    private static final long serialVersionUID = 6650243610865501435L;

    /**
     * Number of milliseconds.
     */
    protected ExprNode millisecondsExpr;

    /**
     * Number of count-to max.
     */
    protected ExprNode numCountToExpr;

    /**
     * For converting matched-events maps to events-per-stream.
     */
    protected transient MatchedEventConvertor convertor;

    public void setGuardParameters(List<ExprNode> parameters, MatchedEventConvertor convertor) throws GuardParameterException
    {
        String message = "Timer-within-or-max-count guard requires two parameters: "
                    + "numeric or time period parameter and an integer-value expression parameter";

        if (parameters.size() != 2) {
            throw new GuardParameterException(message);
        }

        if (!JavaClassHelper.isNumeric(parameters.get(0).getExprEvaluator().getType())) {
            throw new GuardParameterException(message);
        }

        if (parameters.get(1).getExprEvaluator().getType() != Integer.class) {
            throw new GuardParameterException(message);
        }

        this.millisecondsExpr = parameters.get(0);
        this.numCountToExpr = parameters.get(1);
        this.convertor = convertor;
    }

    public long computeMilliseconds(MatchedEventMap beginState, PatternAgentInstanceContext context) {
        if (millisecondsExpr instanceof ExprTimePeriod) {
            ExprTimePeriod timePeriod = (ExprTimePeriod) millisecondsExpr;
            return timePeriod.nonconstEvaluator().deltaMillisecondsUseEngineTime(convertor.convert(beginState), context.getAgentInstanceContext());
        }
        else {
            Object millisecondVal = PatternExpressionUtil.evaluate("Timer-Within-Or-Max-Count guard", beginState, millisecondsExpr, convertor, context.getAgentInstanceContext());
            if (null == millisecondVal) {
                throw new EPException("Timer-within-or-max first parameter evaluated to a null-value");
            }
            Number param = (Number) millisecondVal;
            return Math.round(1000d * param.doubleValue());
        }
    }

    public int computeNumCountTo(MatchedEventMap beginState, PatternAgentInstanceContext context) {
        Object numCountToVal = PatternExpressionUtil.evaluate("Timer-Within-Or-Max-Count guard", beginState, numCountToExpr, convertor,context.getAgentInstanceContext());
        if (null == numCountToVal) {
            throw new EPException("Timer-within-or-max second parameter evaluated to a null-value");
        }
        return (Integer) numCountToVal;
    }

    public Guard makeGuard(PatternAgentInstanceContext context, MatchedEventMap beginState, Quitable quitable, EvalStateNodeNumber stateNodeId, Object guardState)
    {
        return new TimerWithinOrMaxCountGuard(computeMilliseconds(beginState, context), computeNumCountTo(beginState, context), quitable);
    }
}
