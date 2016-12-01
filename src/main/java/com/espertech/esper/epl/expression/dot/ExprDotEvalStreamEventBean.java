/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.expression.dot;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.epl.expression.core.ExprEvaluator;
import com.espertech.esper.epl.expression.core.ExprEvaluatorContext;
import com.espertech.esper.epl.rettype.EPTypeHelper;
import com.espertech.esper.metrics.instrumentation.InstrumentationHelper;

public class ExprDotEvalStreamEventBean implements ExprEvaluator
{
    private final ExprDotNode exprDotNode;
    private final int streamNumber;
    private final ExprDotEval[] evaluators;

    public ExprDotEvalStreamEventBean(ExprDotNode exprDotNode, int streamNumber, ExprDotEval[] evaluators) {
        this.exprDotNode = exprDotNode;
        this.streamNumber = streamNumber;
        this.evaluators = evaluators;
    }

    public Class getType()
    {
        return EPTypeHelper.getClassSingleValued(evaluators[evaluators.length - 1].getTypeInfo());
    }

    public Object evaluate(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext exprEvaluatorContext)
	{
        if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().qExprStreamEventMethod(exprDotNode);}

        EventBean theEvent = eventsPerStream[streamNumber];
        if (theEvent == null) {
            if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().aExprStreamEventMethod(null);}
            return null;
        }

        if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().qExprDotChain(EPTypeHelper.singleEvent(theEvent.getEventType()), theEvent, evaluators);}
        Object inner = ExprDotNodeUtility.evaluateChain(evaluators, theEvent, eventsPerStream, isNewData, exprEvaluatorContext);
        if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().aExprDotChain();}

        if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().aExprStreamEventMethod(inner);}
        return inner;
    }
}
