/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.client.soda;

import java.io.StringWriter;

/**
 * Current execution context supplies the current expression execution context.
 */
public class CurrentEvaluationContextExpression extends ExpressionBase
{
    private static final long serialVersionUID = 1168797704683988017L;

    /**
     * Ctor.
     */
    public CurrentEvaluationContextExpression()
    {
    }

    public ExpressionPrecedenceEnum getPrecedence()
    {
        return ExpressionPrecedenceEnum.UNARY;
    }

    public void toPrecedenceFreeEPL(StringWriter writer)
    {
        writer.write("current_evaluation_context()");
    }
}
