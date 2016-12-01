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
import com.espertech.esper.epl.expression.core.ExprValidationContext;
import com.espertech.esper.epl.expression.core.ExprValidationException;
import com.espertech.esper.epl.expression.baseagg.ExprAggregateNode;
import com.espertech.esper.epl.expression.baseagg.ExprAggregateNodeBase;

/**
 * Represents the leaving() aggregate function is an expression tree.
 */
public class ExprLeavingAggNode extends ExprAggregateNodeBase
{
    private static final long serialVersionUID = -261718190573572758L;

    /**
     * Ctor.
     * @param distinct - flag indicating unique or non-unique value aggregation
     */
    public ExprLeavingAggNode(boolean distinct)
    {
        super(distinct);
    }

    public AggregationMethodFactory validateAggregationChild(ExprValidationContext validationContext) throws ExprValidationException
    {
        if (positionalParams.length > 0) {
            throw makeExceptionExpectedParamNum(0, 0);
        }

        return new ExprLeavingAggNodeFactory(this);
    }

    public String getAggregationFunctionName()
    {
        return "leaving";
    }

    public final boolean equalsNodeAggregateMethodOnly(ExprAggregateNode node)
    {
        return node instanceof ExprLeavingAggNode;
    }
}