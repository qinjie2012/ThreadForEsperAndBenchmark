/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.join.plan;

import com.espertech.esper.epl.expression.core.ExprNode;
import com.espertech.esper.epl.expression.core.ExprNodeUtility;

import java.io.Serializable;

public class QueryGraphValueEntryInKeywordSingleIdx implements QueryGraphValueEntry, Serializable
{
    private static final long serialVersionUID = -2340719032845201999L;
    private final ExprNode[] keyExprs;

    protected QueryGraphValueEntryInKeywordSingleIdx(ExprNode[] keyExprs) {
        this.keyExprs = keyExprs;
    }

    public ExprNode[] getKeyExprs() {
        return keyExprs;
    }

    public String toQueryPlan() {
        return "in-keyword single-indexed multiple key lookup " + ExprNodeUtility.toExpressionStringMinPrecedence(keyExprs);
    }
}

