/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.agg.service;

import com.espertech.esper.epl.expression.core.ExprEvaluator;

/**
 * All aggregation services require evaluation nodes which supply the value to be aggregated (summed, averaged, etc.)
 * and aggregation state factories to make new aggregation states.
 */
public abstract class AggregationServiceFactoryBase implements AggregationServiceFactory
{
    /**
     * Evaluation nodes under.
     */
    protected ExprEvaluator evaluators[];

    /**
     * Prototype aggregation states and factories.
     */
    protected AggregationMethodFactory aggregators[];

    protected Object groupKeyBinding;

    /**
     * Ctor.
     * @param evaluators - are the child node of each aggregation function used for computing the value to be aggregated
     * @param aggregators - aggregation states/factories
     */
    public AggregationServiceFactoryBase(ExprEvaluator evaluators[], AggregationMethodFactory aggregators[], Object groupKeyBinding)
    {
        this.evaluators = evaluators;
        this.aggregators = aggregators;
        this.groupKeyBinding = groupKeyBinding;

        if (evaluators.length != aggregators.length)
        {
            throw new IllegalArgumentException("Expected the same number of evaluates as computer prototypes");
        }
    }
}
