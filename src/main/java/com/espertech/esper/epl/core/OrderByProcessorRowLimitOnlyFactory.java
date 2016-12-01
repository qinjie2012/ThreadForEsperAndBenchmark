/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.core;

import com.espertech.esper.core.context.util.AgentInstanceContext;
import com.espertech.esper.epl.agg.service.AggregationService;
import com.espertech.esper.epl.expression.core.ExprValidationException;
import com.espertech.esper.epl.spec.RowLimitSpec;
import com.espertech.esper.epl.variable.VariableMetaData;
import com.espertech.esper.epl.variable.VariableReader;
import com.espertech.esper.epl.variable.VariableService;
import com.espertech.esper.epl.variable.VariableServiceUtil;
import com.espertech.esper.util.JavaClassHelper;

/**
 * An order-by processor that sorts events according to the expressions
 * in the order_by clause.
 */
public class OrderByProcessorRowLimitOnlyFactory implements OrderByProcessorFactory {

    private final RowLimitProcessorFactory rowLimitProcessorFactory;

    public OrderByProcessorRowLimitOnlyFactory(RowLimitProcessorFactory rowLimitProcessorFactory) {
        this.rowLimitProcessorFactory = rowLimitProcessorFactory;
    }

    public OrderByProcessor instantiate(AggregationService aggregationService, AgentInstanceContext agentInstanceContext) {
        RowLimitProcessor rowLimitProcessor = rowLimitProcessorFactory.instantiate(agentInstanceContext);
        return new OrderByProcessorRowLimitOnly(rowLimitProcessor);
    }
}
