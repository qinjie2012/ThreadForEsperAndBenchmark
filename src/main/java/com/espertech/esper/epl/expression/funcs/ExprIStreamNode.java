/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.expression.funcs;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.epl.expression.core.*;
import com.espertech.esper.metrics.instrumentation.InstrumentationHelper;

import java.io.StringWriter;

/**
 * Represents the RSTREAM() function in an expression tree.
 */
public class ExprIStreamNode extends ExprNodeBase implements ExprEvaluator
{
    private static final long serialVersionUID = -6911351346095189882L;

    /**
     * Ctor.
     */
    public ExprIStreamNode()
    {
    }

    public ExprEvaluator getExprEvaluator()
    {
        return this;
    }

    public ExprNode validate(ExprValidationContext validationContext) throws ExprValidationException
    {
        if (this.getChildNodes().length != 0)
        {
            throw new ExprValidationException("current_timestamp function node must have exactly 1 child node");
        }
        return null;
    }

    public boolean isConstantResult()
    {
        return false;
    }

    public Class getType()
    {
        return Boolean.class;
    }

    public Object evaluate(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext exprEvaluatorContext)
    {
        if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().qaExprIStream(this, isNewData);}
        return isNewData;
    }

    public void toPrecedenceFreeEPL(StringWriter writer) {
        writer.append("istream()");
    }

    public ExprPrecedenceEnum getPrecedence() {
        return ExprPrecedenceEnum.UNARY;
    }

    public boolean equalsNode(ExprNode node)
    {
        if (!(node instanceof ExprIStreamNode))
        {
            return false;
        }
        return true;
    }
}
