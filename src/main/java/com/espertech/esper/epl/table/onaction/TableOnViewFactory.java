/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.table.onaction;

import com.espertech.esper.core.context.util.AgentInstanceContext;
import com.espertech.esper.epl.core.ResultSetProcessor;
import com.espertech.esper.epl.lookup.SubordWMatchExprLookupStrategy;
import com.espertech.esper.epl.lookup.SubordWMatchExprLookupStrategyFactory;
import com.espertech.esper.epl.table.mgmt.TableStateInstance;

public interface TableOnViewFactory
{
    public TableOnViewBase make(SubordWMatchExprLookupStrategy lookupStrategy,
                                TableStateInstance tableState,
                                AgentInstanceContext agentInstanceContext,
                                ResultSetProcessor resultSetProcessor);
}
