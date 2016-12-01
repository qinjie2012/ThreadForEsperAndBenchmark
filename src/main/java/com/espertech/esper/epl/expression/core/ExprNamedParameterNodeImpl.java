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

import java.io.StringWriter;

public class ExprNamedParameterNodeImpl extends ExprNodeBase implements ExprNamedParameterNode, ExprEvaluator {
    private static final long serialVersionUID = -7566189525627783543L;
    private final String parameterName;

    public ExprNamedParameterNodeImpl(String parameterName) {
        this.parameterName = parameterName;
    }

    public String getParameterName() {
        return parameterName;
    }

    public void toPrecedenceFreeEPL(StringWriter writer) {
        writer.append(parameterName);
        writer.append(":");
        if (this.getChildNodes().length > 1) {
            writer.append("(");
        }
        ExprNodeUtility.toExpressionStringParameterList(this.getChildNodes(), writer);
        if (this.getChildNodes().length > 1) {
            writer.append(")");
        }
    }

    public ExprEvaluator getExprEvaluator() {
        return this;
    }

    public ExprPrecedenceEnum getPrecedence() {
        return ExprPrecedenceEnum.UNARY;
    }

    public boolean isConstantResult() {
        return false;
    }

    public boolean equalsNode(ExprNode other) {
        if (!(other instanceof ExprNamedParameterNode)) {
            return false;
        }
        ExprNamedParameterNode otherNamed = (ExprNamedParameterNode) other;
        return otherNamed.getParameterName().equals(parameterName);
    }

    public ExprNode validate(ExprValidationContext validationContext) throws ExprValidationException {
        return null;
    }

    public Object evaluate(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context) {
        return null;
    }

    public Class getType() {
        return null;
    }
}
