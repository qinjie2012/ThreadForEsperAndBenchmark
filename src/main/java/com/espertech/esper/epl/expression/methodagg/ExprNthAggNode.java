/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.expression.methodagg;

import com.espertech.esper.epl.agg.service.AggregationMethodFactory;
import com.espertech.esper.epl.expression.core.ExprNode;
import com.espertech.esper.epl.expression.core.ExprValidationContext;
import com.espertech.esper.epl.expression.core.ExprValidationException;
import com.espertech.esper.epl.expression.baseagg.ExprAggregateNode;
import com.espertech.esper.epl.expression.baseagg.ExprAggregateNodeBase;

/**
 * Represents the nth(...) and aggregate function is an expression tree.
 */
public class ExprNthAggNode extends ExprAggregateNodeBase
{
    private static final long serialVersionUID = -843689785630260527L;

    /**
     * Ctor.
     * @param distinct - flag indicating unique or non-unique value aggregation
     */
    public ExprNthAggNode(boolean distinct)
    {
        super(distinct);
    }

    public AggregationMethodFactory validateAggregationChild(ExprValidationContext validationContext) throws ExprValidationException
    {
        String message = "The nth aggregation function requires two parameters, an expression returning aggregation values and a numeric index constant";
        if (this.positionalParams.length != 2) {
            throw new ExprValidationException(message);
        }

        ExprNode first = this.positionalParams[0];
        ExprNode second = this.positionalParams[1];
        if (!second.isConstantResult()) {
            throw new ExprValidationException(message);
        }

        Number num = (Number) second.getExprEvaluator().evaluate(null, true, validationContext.getExprEvaluatorContext());
        int size = num.intValue();

        return new ExprNthAggNodeFactory(this, first.getExprEvaluator().getType(), size);
    }

    public String getAggregationFunctionName()
    {
        return "nth";
    }

    public final boolean equalsNodeAggregateMethodOnly(ExprAggregateNode node)
    {
        return node instanceof ExprNthAggNode;
    }
}