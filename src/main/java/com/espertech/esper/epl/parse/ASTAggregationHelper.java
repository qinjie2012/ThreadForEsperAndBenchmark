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

package com.espertech.esper.epl.parse;

import com.espertech.esper.client.ConfigurationPlugInAggregationMultiFunction;
import com.espertech.esper.client.hook.AggregationFunctionFactory;
import com.espertech.esper.epl.core.EngineImportException;
import com.espertech.esper.epl.core.EngineImportService;
import com.espertech.esper.epl.core.EngineImportUndefinedException;
import com.espertech.esper.epl.expression.core.ExprNode;
import com.espertech.esper.epl.expression.methodagg.ExprPlugInAggFunctionFactoryNode;
import com.espertech.esper.epl.expression.accessagg.ExprPlugInAggMultiFunctionNode;
import com.espertech.esper.plugin.PlugInAggregationMultiFunctionDeclarationContext;
import com.espertech.esper.plugin.PlugInAggregationMultiFunctionFactory;
import com.espertech.esper.util.JavaClassHelper;
import com.espertech.esper.util.LazyAllocatedMap;

public class ASTAggregationHelper {
    public static ExprNode tryResolveAsAggregation(EngineImportService engineImportService,
                                             boolean distinct,
                                             String functionName,
                                             LazyAllocatedMap<ConfigurationPlugInAggregationMultiFunction, PlugInAggregationMultiFunctionFactory> plugInAggregations,
                                             String engineURI) {
        try
        {
            AggregationFunctionFactory aggregationFactory = engineImportService.resolveAggregationFactory(functionName);
            return new ExprPlugInAggFunctionFactoryNode(distinct, aggregationFactory, functionName);
        }
        catch (EngineImportUndefinedException e)
        {
            // Not an aggregation function
        }
        catch (EngineImportException e)
        {
            throw new IllegalStateException("Error resolving aggregation: " + e.getMessage(), e);
        }

        // try plug-in aggregation multi-function
        ConfigurationPlugInAggregationMultiFunction config = engineImportService.resolveAggregationMultiFunction(functionName);
        if (config != null) {
            PlugInAggregationMultiFunctionFactory factory = plugInAggregations.getMap().get(config);
            if (factory == null) {
                factory = (PlugInAggregationMultiFunctionFactory) JavaClassHelper.instantiate(PlugInAggregationMultiFunctionFactory.class, config.getMultiFunctionFactoryClassName());
                plugInAggregations.getMap().put(config, factory);
            }
            factory.addAggregationFunction(new PlugInAggregationMultiFunctionDeclarationContext(functionName.toLowerCase(), distinct, engineURI, config));
            return new ExprPlugInAggMultiFunctionNode(distinct, config, factory, functionName);
        }

        // try built-in expanded set of aggregation functions
        return engineImportService.resolveAggExtendedBuiltin(functionName, distinct);
    }
}
