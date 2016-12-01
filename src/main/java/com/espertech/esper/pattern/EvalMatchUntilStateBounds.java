/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.pattern;

import com.espertech.esper.client.EventBean;

public class EvalMatchUntilStateBounds
{
    private final Integer lowerbounds;
    private final Integer upperbounds;

    public EvalMatchUntilStateBounds(Integer lowerbounds, Integer upperbounds) {
        this.lowerbounds = lowerbounds;
        this.upperbounds = upperbounds;
    }

    public Integer getLowerbounds() {
        return lowerbounds;
    }

    public Integer getUpperbounds() {
        return upperbounds;
    }
    
    public static EvalMatchUntilStateBounds initBounds(EvalMatchUntilFactoryNode factoryNode, MatchedEventMap beginState, PatternAgentInstanceContext context) {
        Integer lowerbounds = null;
        Integer upperbounds = null;
        EventBean[] eventsPerStream = factoryNode.getConvertor().convert(beginState);
        if (factoryNode.getSingleBound() != null) {
            Integer bounds = (Integer) factoryNode.getSingleBound().getExprEvaluator().evaluate(eventsPerStream, true, context.getAgentInstanceContext());
            lowerbounds = bounds;
            upperbounds = bounds;
        }
        else {
            if (factoryNode.getLowerBounds() != null) {
                lowerbounds = (Integer) factoryNode.getLowerBounds().getExprEvaluator().evaluate(eventsPerStream, true, context.getAgentInstanceContext());
            }
            if (factoryNode.getUpperBounds() != null) {
                upperbounds = (Integer) factoryNode.getUpperBounds().getExprEvaluator().evaluate(eventsPerStream, true, context.getAgentInstanceContext());
            }
            if (upperbounds != null && lowerbounds != null) {
                if (upperbounds < lowerbounds) {
                    Integer lbounds =  lowerbounds;
                    lowerbounds = upperbounds;
                    upperbounds = lbounds;
                }
            }
        }
        return new EvalMatchUntilStateBounds(lowerbounds, upperbounds);
    }
}
