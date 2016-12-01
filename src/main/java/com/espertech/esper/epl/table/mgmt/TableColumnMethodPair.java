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

package com.espertech.esper.epl.table.mgmt;

import com.espertech.esper.epl.expression.core.ExprEvaluator;
import com.espertech.esper.epl.expression.core.ExprNode;

public class TableColumnMethodPair {
    private final ExprEvaluator evaluator;
    private final int targetIndex;
    private final ExprNode aggregationNode;

    public TableColumnMethodPair(ExprEvaluator evaluator, int targetIndex, ExprNode aggregationNode) {
        this.evaluator = evaluator;
        this.targetIndex = targetIndex;
        this.aggregationNode = aggregationNode;
    }

    public ExprEvaluator getEvaluator() {
        return evaluator;
    }

    public int getTargetIndex() {
        return targetIndex;
    }

    public ExprNode getAggregationNode() {
        return aggregationNode;
    }
}
