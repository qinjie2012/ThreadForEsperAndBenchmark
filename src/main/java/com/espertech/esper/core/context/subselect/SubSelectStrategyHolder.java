/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.core.context.subselect;

import com.espertech.esper.core.context.factory.StatementAgentInstancePostLoad;
import com.espertech.esper.epl.agg.service.AggregationService;
import com.espertech.esper.epl.expression.prev.ExprPreviousEvalStrategy;
import com.espertech.esper.epl.expression.prev.ExprPreviousNode;
import com.espertech.esper.epl.expression.prior.ExprPriorEvalStrategy;
import com.espertech.esper.epl.expression.prior.ExprPriorNode;
import com.espertech.esper.epl.expression.subquery.ExprSubselectStrategy;
import com.espertech.esper.view.Viewable;

import java.util.Map;

/**
 * Entry holding lookup resource references for use by {@link SubSelectActivationCollection}.
 */
public class SubSelectStrategyHolder
{
    private final ExprSubselectStrategy stategy;
    private final AggregationService subselectAggregationService;
    private final Map<ExprPriorNode, ExprPriorEvalStrategy> priorStrategies;
    private final Map<ExprPreviousNode, ExprPreviousEvalStrategy> previousNodeStrategies;
    private final Viewable subselectView;
    private final StatementAgentInstancePostLoad postLoad;

    public SubSelectStrategyHolder(ExprSubselectStrategy stategy, AggregationService subselectAggregationService, Map<ExprPriorNode, ExprPriorEvalStrategy> priorStrategies, Map<ExprPreviousNode, ExprPreviousEvalStrategy> previousNodeStrategies, Viewable subselectView, StatementAgentInstancePostLoad postLoad) {
        this.stategy = stategy;
        this.subselectAggregationService = subselectAggregationService;
        this.priorStrategies = priorStrategies;
        this.previousNodeStrategies = previousNodeStrategies;
        this.subselectView = subselectView;
        this.postLoad = postLoad;
    }

    public ExprSubselectStrategy getStategy() {
        return stategy;
    }

    public AggregationService getSubselectAggregationService() {
        return subselectAggregationService;
    }

    public Map<ExprPriorNode, ExprPriorEvalStrategy> getPriorStrategies() {
        return priorStrategies;
    }

    public Map<ExprPreviousNode, ExprPreviousEvalStrategy> getPreviousNodeStrategies() {
        return previousNodeStrategies;
    }

    public Viewable getSubselectView() {
        return subselectView;
    }

    public StatementAgentInstancePostLoad getPostLoad() {
        return postLoad;
    }
}
