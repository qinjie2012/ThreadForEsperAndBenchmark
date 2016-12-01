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

public class ExprGroupingNode extends ExprNodeBase implements ExprEvaluator {

    private static final long serialVersionUID = 8054177261371678105L;

    public ExprNode validate(ExprValidationContext validationContext) throws ExprValidationException {
        if (!validationContext.isAllowRollupFunctions()) {
            throw ExprGroupingIdNode.makeException("grouping");
        }
        return null;
    }

    public ExprEvaluator getExprEvaluator() {
        return this;
    }

    public void toPrecedenceFreeEPL(StringWriter writer) {
        ExprNodeUtility.toExpressionStringWFunctionName("grouping", this.getChildNodes(), writer);
    }

    public ExprPrecedenceEnum getPrecedence() {
        return ExprPrecedenceEnum.UNARY;
    }

    public boolean isConstantResult() {
        return false;
    }

    public boolean equalsNode(ExprNode node) {
        return false;
    }

    public Object evaluate(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context) {
        return null;
    }

    public Class getType() {
        return Integer.class;
    }
}
