/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.expression.accessagg;

import com.espertech.esper.client.ConfigurationPlugInAggregationMultiFunction;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventType;
import com.espertech.esper.epl.agg.service.AggregationMethodFactory;
import com.espertech.esper.epl.enummethod.dot.ArrayWrappingCollection;
import com.espertech.esper.epl.expression.baseagg.ExprAggregateNode;
import com.espertech.esper.epl.expression.baseagg.ExprAggregateNodeBase;
import com.espertech.esper.epl.expression.baseagg.ExprAggregationPlugInNodeMarker;
import com.espertech.esper.epl.expression.core.*;
import com.espertech.esper.epl.table.mgmt.TableMetadataColumnAggregation;
import com.espertech.esper.event.EventAdapterService;
import com.espertech.esper.plugin.PlugInAggregationMultiFunctionFactory;
import com.espertech.esper.plugin.PlugInAggregationMultiFunctionHandler;
import com.espertech.esper.plugin.PlugInAggregationMultiFunctionValidationContext;

import java.util.Collection;

/**
 * Represents a custom aggregation function in an expresson tree.
 */
public class ExprPlugInAggMultiFunctionNode extends ExprAggregateNodeBase implements ExprEvaluatorEnumeration, ExprAggregateAccessMultiValueNode, ExprAggregationPlugInNodeMarker
{
    private static final long serialVersionUID = 6356766499476980697L;
    private PlugInAggregationMultiFunctionFactory pluginAggregationMultiFunctionFactory;
    private final String functionName;
    private final ConfigurationPlugInAggregationMultiFunction config;
    private transient ExprPlugInAggMultiFunctionNodeFactory factory;

    /**
     * Ctor.
     * @param distinct - flag indicating unique or non-unique value aggregation
     * @param pluginAggregationMultiFunctionFactory - the factory
     * @param functionName is the aggregation function name
     */
    public ExprPlugInAggMultiFunctionNode(boolean distinct, ConfigurationPlugInAggregationMultiFunction config, PlugInAggregationMultiFunctionFactory pluginAggregationMultiFunctionFactory, String functionName)
    {
        super(distinct);
        this.pluginAggregationMultiFunctionFactory = pluginAggregationMultiFunctionFactory;
        this.functionName = functionName;
        this.config = config;
    }

    public AggregationMethodFactory validateAggregationChild(ExprValidationContext validationContext) throws ExprValidationException {
        validatePositionals();
        return validateAggregationParamsWBinding(validationContext, null);
    }

    public AggregationMethodFactory validateAggregationParamsWBinding(ExprValidationContext validationContext, TableMetadataColumnAggregation tableAccessColumn) throws ExprValidationException {
        // validate using the context provided by the 'outside' streams to determine parameters
        // at this time 'inside' expressions like 'window(intPrimitive)' are not handled
        ExprNodeUtility.getValidatedSubtree(ExprNodeOrigin.AGGPARAM, this.getChildNodes(), validationContext);
        return validateAggregationInternal(validationContext, tableAccessColumn);
    }

    private AggregationMethodFactory validateAggregationInternal(ExprValidationContext validationContext, TableMetadataColumnAggregation optionalTableColumn) throws ExprValidationException {
        PlugInAggregationMultiFunctionValidationContext ctx = new PlugInAggregationMultiFunctionValidationContext(functionName, validationContext.getStreamTypeService().getEventTypes(), positionalParams, validationContext.getStreamTypeService().getEngineURIQualifier(), validationContext.getStatementName(), validationContext, config, optionalTableColumn, getChildNodes());
        PlugInAggregationMultiFunctionHandler handlerPlugin = pluginAggregationMultiFunctionFactory.validateGetHandler(ctx);
        factory = new ExprPlugInAggMultiFunctionNodeFactory(this, handlerPlugin);
        return factory;
    }

    public String getAggregationFunctionName()
    {
        return functionName;
    }

    public Collection<EventBean> evaluateGetROCollectionEvents(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context) {
        return super.aggregationResultFuture.getCollectionOfEvents(column, eventsPerStream, isNewData, context);
    }

    public Collection evaluateGetROCollectionScalar(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context) {
        Object result = super.aggregationResultFuture.getValue(column, context.getAgentInstanceId(), eventsPerStream, isNewData, context);
        if (result == null) {
            return null;
        }
        if (result.getClass().isArray()) {
            return new ArrayWrappingCollection(result);
        }
        return (Collection) result;
    }

    public EventType getEventTypeCollection(EventAdapterService eventAdapterService, String statementId) {
        return factory.getEventTypeCollection();
    }

    public Class getComponentTypeCollection() throws ExprValidationException {
        return factory.getComponentTypeCollection();
    }

    public EventType getEventTypeSingle(EventAdapterService eventAdapterService, String statementId) throws ExprValidationException {
        return factory.getEventTypeSingle();
    }

    public EventBean evaluateGetEventBean(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context) {
        return super.aggregationResultFuture.getEventBean(column, eventsPerStream, isNewData, context);
    }

    public final boolean equalsNodeAggregateMethodOnly(ExprAggregateNode node)
    {
        return false;
    }
}
