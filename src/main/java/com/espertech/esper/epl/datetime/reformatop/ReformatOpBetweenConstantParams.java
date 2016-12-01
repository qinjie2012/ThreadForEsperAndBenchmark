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

package com.espertech.esper.epl.datetime.reformatop;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventType;
import com.espertech.esper.epl.datetime.eval.DatetimeLongCoercerFactory;
import com.espertech.esper.epl.datetime.eval.DatetimeMethodEnum;
import com.espertech.esper.epl.datetime.eval.ExprDotNodeFilterAnalyzerDesc;
import com.espertech.esper.epl.expression.dot.ExprDotNodeFilterAnalyzerInput;
import com.espertech.esper.epl.expression.core.ExprEvaluatorContext;
import com.espertech.esper.epl.expression.core.ExprNode;
import com.espertech.esper.epl.expression.core.ExprValidationException;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class ReformatOpBetweenConstantParams implements ReformatOp {

    private long first;
    private long second;

    public ReformatOpBetweenConstantParams(List<ExprNode> parameters) throws ExprValidationException {
        long paramFirst = getLongValue(parameters.get(0));
        long paramSecond = getLongValue(parameters.get(1));

        if (paramFirst > paramSecond) {
            this.second = paramFirst;
            this.first = paramSecond;
        }
        else {
            this.first = paramFirst;
            this.second = paramSecond;
        }
        if (parameters.size() > 2) {
            if (!getBooleanValue(parameters.get(2))) {
                first = first + 1;
            }
            if (!getBooleanValue(parameters.get(3))) {
                second = second - 1;
            }
        }
    }

    private long getLongValue(ExprNode exprNode)
        throws ExprValidationException
    {
        Object value = exprNode.getExprEvaluator().evaluate(null, true, null);
        if (value == null) {
            throw new ExprValidationException("Date-time method 'between' requires non-null parameter values");
        }
        return DatetimeLongCoercerFactory.getCoercer(value.getClass()).coerce(value);
    }

    private boolean getBooleanValue(ExprNode exprNode)
        throws ExprValidationException
    {
        Object value = exprNode.getExprEvaluator().evaluate(null, true, null);
        if (value == null) {
            throw new ExprValidationException("Date-time method 'between' requires non-null parameter values");
        }
        return (Boolean) value;
    }

    public Object evaluate(Long ts, EventBean[] eventsPerStream, boolean newData, ExprEvaluatorContext exprEvaluatorContext) {
        if (ts == null) {
            return null;
        }
        return evaluateInternal(ts);
    }

    public Object evaluate(Date d, EventBean[] eventsPerStream, boolean newData, ExprEvaluatorContext exprEvaluatorContext) {
        if (d == null) {
            return null;
        }
        return evaluateInternal(d.getTime());
    }

    public Object evaluate(Calendar cal, EventBean[] eventsPerStream, boolean newData, ExprEvaluatorContext exprEvaluatorContext) {
        if (cal == null) {
            return null;
        }
        return evaluateInternal(cal.getTimeInMillis());
    }

    public Object evaluateInternal(long ts) {
        return first <= ts && ts <= second;
    }

    public Class getReturnType() {
        return Boolean.class;
    }

    public ExprDotNodeFilterAnalyzerDesc getFilterDesc(EventType[] typesPerStream, DatetimeMethodEnum currentMethod, List<ExprNode> currentParameters, ExprDotNodeFilterAnalyzerInput inputDesc) {
        return null;
    }
}
