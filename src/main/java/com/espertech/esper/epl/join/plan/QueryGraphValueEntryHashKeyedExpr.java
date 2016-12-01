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

public class QueryGraphValueEntryHashKeyedExpr extends QueryGraphValueEntryHashKeyed
{
    private static final long serialVersionUID = 1069112032977675596L;

    private final boolean requiresKey;

    public QueryGraphValueEntryHashKeyedExpr(ExprNode keyExpr, boolean requiresKey) {
        super(keyExpr);
        this.requiresKey = requiresKey;
    }

    public boolean isRequiresKey() {
        return requiresKey;
    }

    public boolean isConstant() {
        return ExprNodeUtility.isConstantValueExpr(super.getKeyExpr());
    }

    public String toQueryPlan() {
        return ExprNodeUtility.toExpressionStringMinPrecedenceSafe(getKeyExpr());
    }
}

