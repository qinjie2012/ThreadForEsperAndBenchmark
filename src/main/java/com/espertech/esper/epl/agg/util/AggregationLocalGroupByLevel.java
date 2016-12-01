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

package com.espertech.esper.epl.agg.util;

import com.espertech.esper.epl.agg.service.AggregationMethodFactory;
import com.espertech.esper.epl.agg.service.AggregationStateFactory;
import com.espertech.esper.epl.expression.core.ExprEvaluator;

public class AggregationLocalGroupByLevel {

    private final ExprEvaluator[] methodEvaluators;
    private final AggregationMethodFactory[] methodFactories;
    private final AggregationStateFactory[] stateFactories;
    private final ExprEvaluator[] partitionEvaluators;
    private final boolean isDefaultLevel;

    public AggregationLocalGroupByLevel(ExprEvaluator[] methodEvaluators, AggregationMethodFactory[] methodFactories, AggregationStateFactory[] stateFactories, ExprEvaluator[] partitionEvaluators, boolean defaultLevel) {
        this.methodEvaluators = methodEvaluators;
        this.methodFactories = methodFactories;
        this.stateFactories = stateFactories;
        this.partitionEvaluators = partitionEvaluators;
        isDefaultLevel = defaultLevel;
    }

    public ExprEvaluator[] getMethodEvaluators() {
        return methodEvaluators;
    }

    public AggregationMethodFactory[] getMethodFactories() {
        return methodFactories;
    }

    public AggregationStateFactory[] getStateFactories() {
        return stateFactories;
    }

    public ExprEvaluator[] getPartitionEvaluators() {
        return partitionEvaluators;
    }

    public boolean isDefaultLevel() {
        return isDefaultLevel;
    }
}
