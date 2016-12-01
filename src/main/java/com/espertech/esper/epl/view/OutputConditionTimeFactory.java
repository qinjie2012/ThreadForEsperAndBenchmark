/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.view;

import com.espertech.esper.core.context.util.AgentInstanceContext;
import com.espertech.esper.core.service.StatementContext;
import com.espertech.esper.epl.expression.time.ExprTimePeriod;
import com.espertech.esper.epl.expression.time.ExprTimePeriodEvalDeltaNonConst;

/**
 * Output condition that is satisfied at the end
 * of every time interval of a given length.
 */
public final class OutputConditionTimeFactory implements OutputConditionFactory
{
    private final ExprTimePeriod timePeriod;
    private final ExprTimePeriodEvalDeltaNonConst timePeriodDeltaComputation;
    private final boolean isStartConditionOnCreation;

    /**
     * Constructor.
     * @param timePeriod is the number of minutes or seconds to batch events for, may include variables
     */
    public OutputConditionTimeFactory(ExprTimePeriod timePeriod,
                                      boolean isStartConditionOnCreation)
    {
        this.timePeriod = timePeriod;
        this.timePeriodDeltaComputation = timePeriod.nonconstEvaluator();
        this.isStartConditionOnCreation = isStartConditionOnCreation;
    }

    public OutputCondition make(AgentInstanceContext agentInstanceContext, OutputCallback outputCallback) {
        return new OutputConditionTime(outputCallback, agentInstanceContext, this, isStartConditionOnCreation);
    }

    public ExprTimePeriod getTimePeriod() {
        return timePeriod;
    }

    public ExprTimePeriodEvalDeltaNonConst getTimePeriodDeltaComputation() {
        return timePeriodDeltaComputation;
    }
}
