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

/**
 * A placeholder expression for view/pattern object parameters that allow
 * sorting expression values ascending or descending.
 */
public class ExprOrderedExpr extends ExprNodeBase implements ExprEvaluator
{
    private final boolean isDescending;
    private transient ExprEvaluator evaluator;
    private static final long serialVersionUID = -3140402807682771591L;

    /**
     * Ctor.
     * @param descending is true for descending sorts
     */
    public ExprOrderedExpr(boolean descending)
    {
        isDescending = descending;
    }

    public void toPrecedenceFreeEPL(StringWriter writer) {
        this.getChildNodes()[0].toEPL(writer, ExprPrecedenceEnum.MINIMUM);
        if (isDescending) {
            writer.append(" desc");
        }
    }

    public ExprPrecedenceEnum getPrecedence() {
        return ExprPrecedenceEnum.UNARY;
    }

    public ExprEvaluator getExprEvaluator()
    {
        return this;
    }

    public boolean isConstantResult()
    {
        return getChildNodes()[0].isConstantResult();
    }

    public boolean equalsNode(ExprNode node)
    {
        if (!(node instanceof ExprOrderedExpr))
        {
            return false;
        }
        ExprOrderedExpr other = (ExprOrderedExpr) node;
        return other.isDescending == this.isDescending;
    }

    public ExprNode validate(ExprValidationContext validationContext) throws ExprValidationException
    {
        evaluator = getChildNodes()[0].getExprEvaluator();
        // always valid
        return null;
    }

    public Class getType()
    {
        return getChildNodes()[0].getExprEvaluator().getType();
    }

    public Object evaluate(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext exprEvaluatorContext)
    {
        return evaluator.evaluate(eventsPerStream, isNewData, exprEvaluatorContext);
    }

    /**
     * Returns true for descending sort.
     * @return indicator for ascending or descending sort
     */
    public boolean isDescending()
    {
        return isDescending;
    }
}
