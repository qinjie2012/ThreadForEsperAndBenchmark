/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.expression.accessagg;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventType;
import com.espertech.esper.collection.Pair;
import com.espertech.esper.core.service.StatementType;
import com.espertech.esper.epl.agg.access.*;
import com.espertech.esper.epl.agg.service.AggregationMethodFactory;
import com.espertech.esper.epl.agg.service.AggregationStateKeyWStream;
import com.espertech.esper.epl.agg.service.AggregationStateTypeWStream;
import com.espertech.esper.epl.expression.baseagg.ExprAggregateNode;
import com.espertech.esper.epl.expression.baseagg.ExprAggregateNodeBase;
import com.espertech.esper.epl.expression.core.*;
import com.espertech.esper.epl.table.mgmt.TableMetadata;
import com.espertech.esper.epl.table.mgmt.TableMetadataColumnAggregation;
import com.espertech.esper.event.EventAdapterService;
import com.espertech.esper.util.JavaClassHelper;

import java.io.StringWriter;
import java.util.Collection;
import java.util.Set;

public class ExprAggMultiFunctionSortedMinMaxByNode extends ExprAggregateNodeBase implements ExprEvaluatorEnumeration, ExprAggregateAccessMultiValueNode
{
    private static final long serialVersionUID = -8407756454712340265L;
    private final boolean max;
    private final boolean ever;
    private final boolean sortedwin;

    private transient EventType containedType;

    /**
     * Ctor.
     */
    public ExprAggMultiFunctionSortedMinMaxByNode(boolean max, boolean ever, boolean sortedwin) {
        super(false);
        this.max = max;
        this.ever = ever;
        this.sortedwin = sortedwin;
    }

    public AggregationMethodFactory validateAggregationParamsWBinding(ExprValidationContext validationContext, TableMetadataColumnAggregation tableAccessColumn) throws ExprValidationException {
        return validateAggregationInternal(validationContext, tableAccessColumn);
    }

    public AggregationMethodFactory validateAggregationChild(ExprValidationContext validationContext) throws ExprValidationException {
        return validateAggregationInternal(validationContext, null);
    }

    private AggregationMethodFactory validateAggregationInternal(ExprValidationContext validationContext, TableMetadataColumnAggregation optionalBinding) throws ExprValidationException
    {
        ExprAggMultiFunctionSortedMinMaxByNodeFactory factory;

        // handle table-access expression (state provided, accessor needed)
        if (optionalBinding != null) {
            factory = handleTableAccess(optionalBinding);
        }
        // handle create-table statements (state creator and default accessor, limited to certain options)
        else if (validationContext.getExprEvaluatorContext().getStatementType() == StatementType.CREATE_TABLE) {
            factory = handleCreateTable(validationContext);
        }
        // handle into-table (state provided, accessor and agent needed, validation done by factory)
        else if (validationContext.getIntoTableName() != null) {
            factory = handleIntoTable(validationContext);
        }
        // handle standalone
        else {
            factory = handleNonTable(validationContext);
        }

        this.containedType = factory.getContainedEventType();
        return factory;
    }

    private ExprAggMultiFunctionSortedMinMaxByNodeFactory handleNonTable(ExprValidationContext validationContext)
            throws ExprValidationException
    {
        if (positionalParams.length == 0) {
            throw new ExprValidationException("Missing the sort criteria expression");
        }

        // validate that the streams referenced in the criteria are a single stream's
        Set<Integer> streams = ExprNodeUtility.getIdentStreamNumbers(positionalParams[0]);
        if (streams.size() > 1 || streams.isEmpty()) {
            throw new ExprValidationException(getErrorPrefix() + " requires that any parameter expressions evaluate properties of the same stream");
        }
        int streamNum = streams.iterator().next();

        // validate that there is a remove stream, use "ever" if not
        boolean forceEver = false;
        if (!ever && ExprAggMultiFunctionLinearAccessNode.getIstreamOnly(validationContext.getStreamTypeService(), streamNum)) {
            if (sortedwin) {
                throw new ExprValidationException(getErrorPrefix() + " requires that a data window is declared for the stream");
            }
            forceEver = true;
        }

        // determine typing and evaluation
        containedType = validationContext.getStreamTypeService().getEventTypes()[streamNum];

        Class componentType = containedType.getUnderlyingType();
        Class accessorResultType = componentType;
        AggregationAccessor accessor;
        TableMetadata tableMetadata = validationContext.getTableService().getTableMetadataFromEventType(containedType);
        if (!sortedwin) {
            if (tableMetadata != null) {
                accessor = new AggregationAccessorMinMaxByTable(max, tableMetadata);
            }
            else {
                accessor = new AggregationAccessorMinMaxByNonTable(max);
            }
        }
        else {
            if (tableMetadata != null) {
                accessor = new AggregationAccessorSortedTable(max, componentType, tableMetadata);
            }
            else {
                accessor = new AggregationAccessorSortedNonTable(max, componentType);
            }
            accessorResultType = JavaClassHelper.getArrayType(accessorResultType);
        }

        Pair<ExprNode[], boolean[]> criteriaExpressions = getCriteriaExpressions();

        AggregationStateTypeWStream type;
        if (ever) {
            type = max ? AggregationStateTypeWStream.MAXEVER : AggregationStateTypeWStream.MINEVER;
        }
        else {
            type = AggregationStateTypeWStream.SORTED;
        }
        AggregationStateKeyWStream stateKey = new AggregationStateKeyWStream(streamNum, containedType, type, criteriaExpressions.getFirst());

        SortedAggregationStateFactoryFactory stateFactoryFactory = new
                SortedAggregationStateFactoryFactory(validationContext.getMethodResolutionService(),
                ExprNodeUtility.getEvaluators(criteriaExpressions.getFirst()),
                criteriaExpressions.getSecond(), ever, streamNum, this);

        return new ExprAggMultiFunctionSortedMinMaxByNodeFactory(this, accessor, accessorResultType, containedType, stateKey, stateFactoryFactory, AggregationAgentDefault.INSTANCE);
    }

    private ExprAggMultiFunctionSortedMinMaxByNodeFactory handleIntoTable(ExprValidationContext validationContext)
            throws ExprValidationException
    {
        int streamNum;
        if (positionalParams.length == 0 ||
           (positionalParams.length == 1 && positionalParams[0] instanceof ExprWildcard)) {
            ExprAggMultiFunctionUtil.validateWildcardStreamNumbers(validationContext.getStreamTypeService(), getAggregationFunctionName());
            streamNum = 0;
        }
        else if (positionalParams.length == 1 && positionalParams[0] instanceof ExprStreamUnderlyingNode) {
            streamNum = ExprAggMultiFunctionUtil.validateStreamWildcardGetStreamNum(positionalParams[0]);
        }
        else if (positionalParams.length > 0) {
            throw new ExprValidationException("When specifying into-table a sort expression cannot be provided");
        }
        else {
            streamNum = 0;
        }

        EventType containedType = validationContext.getStreamTypeService().getEventTypes()[streamNum];
        Class componentType = containedType.getUnderlyingType();
        Class accessorResultType = componentType;
        AggregationAccessor accessor;
        if (!sortedwin) {
            accessor = new AggregationAccessorMinMaxByNonTable(max);
        }
        else {
            accessor = new AggregationAccessorSortedNonTable(max, componentType);
            accessorResultType = JavaClassHelper.getArrayType(accessorResultType);
        }

        AggregationAgent agent = AggregationAgentDefault.INSTANCE;
        if (streamNum != 0) {
            agent = new AggregationAgentRewriteStream(streamNum);
        }

        return new ExprAggMultiFunctionSortedMinMaxByNodeFactory(this, accessor, accessorResultType, containedType, null, null, agent);
    }

    private ExprAggMultiFunctionSortedMinMaxByNodeFactory handleCreateTable(ExprValidationContext validationContext)
            throws ExprValidationException
    {
        if (positionalParams.length == 0) {
            throw new ExprValidationException("Missing the sort criteria expression");
        }

        String message = "For tables columns, the aggregation function requires the 'sorted(*)' declaration";
        if (!sortedwin && !ever) {
            throw new ExprValidationException(message);
        }
        if (validationContext.getStreamTypeService().getStreamNames().length == 0) {
            throw new ExprValidationException("'Sorted' requires that the event type is provided");
        }
        EventType containedType = validationContext.getStreamTypeService().getEventTypes()[0];
        Class componentType = containedType.getUnderlyingType();
        Pair<ExprNode[], boolean[]> criteriaExpressions = getCriteriaExpressions();
        Class accessorResultType = componentType;
        AggregationAccessor accessor;
        if (!sortedwin) {
            accessor = new AggregationAccessorMinMaxByNonTable(max);
        }
        else {
            accessor = new AggregationAccessorSortedNonTable(max, componentType);
            accessorResultType = JavaClassHelper.getArrayType(accessorResultType);
        }
        SortedAggregationStateFactoryFactory stateFactoryFactory = new
                SortedAggregationStateFactoryFactory(validationContext.getMethodResolutionService(),
                    ExprNodeUtility.getEvaluators(criteriaExpressions.getFirst()),
                    criteriaExpressions.getSecond(), ever, 0, this);
        return new ExprAggMultiFunctionSortedMinMaxByNodeFactory(this, accessor, accessorResultType, containedType, null, stateFactoryFactory, null);
    }

    private Pair<ExprNode[], boolean[]> getCriteriaExpressions() {
        // determine ordering ascending/descending and build criteria expression without "asc" marker
        ExprNode[] criteriaExpressions = new ExprNode[this.positionalParams.length];
        boolean[] sortDescending = new boolean[positionalParams.length];
        for (int i = 0; i < positionalParams.length; i++) {
            ExprNode parameter = positionalParams[i];
            criteriaExpressions[i] = parameter;
            if (parameter instanceof ExprOrderedExpr) {
                ExprOrderedExpr ordered = (ExprOrderedExpr) parameter;
                sortDescending[i] = ordered.isDescending();
                if (!ordered.isDescending()) {
                    criteriaExpressions[i] = ordered.getChildNodes()[0];
                }
            }
        }
        return new Pair<ExprNode[], boolean[]>(criteriaExpressions, sortDescending);
    }

    private ExprAggMultiFunctionSortedMinMaxByNodeFactory handleTableAccess(TableMetadataColumnAggregation tableAccess) {
        ExprAggMultiFunctionSortedMinMaxByNodeFactory factory = (ExprAggMultiFunctionSortedMinMaxByNodeFactory) tableAccess.getFactory();
        AggregationAccessor accessor;
        Class componentType = factory.getContainedEventType().getUnderlyingType();
        Class accessorResultType = componentType;
        if (!sortedwin) {
            accessor = new AggregationAccessorMinMaxByNonTable(max);
        }
        else {
            accessor = new AggregationAccessorSortedNonTable(max, componentType);
            accessorResultType = JavaClassHelper.getArrayType(accessorResultType);
        }
        return new ExprAggMultiFunctionSortedMinMaxByNodeFactory(this, accessor, accessorResultType, factory.getContainedEventType(), null, null, null);
    }

    public String getAggregationFunctionName() {
        if (sortedwin) {
            return "sorted";
        }
        if (ever) {
            return max ? "maxbyever" : "minbyever";
        }
        return max ? "maxby" : "minby";
    }

    @Override
    public void toPrecedenceFreeEPL(StringWriter writer) {
        writer.append(getAggregationFunctionName());
        ExprNodeUtility.toExpressionStringParams(writer, this.positionalParams);
    }

    public Collection<EventBean> evaluateGetROCollectionEvents(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context) {
        return super.aggregationResultFuture.getCollectionOfEvents(column, eventsPerStream, isNewData, context);
    }

    public Collection evaluateGetROCollectionScalar(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context) {
        return null;
    }

    public EventType getEventTypeCollection(EventAdapterService eventAdapterService, String statementId) {
        if (!sortedwin) {
            return null;
        }
        return containedType;
    }

    public Class getComponentTypeCollection() throws ExprValidationException {
        return null;
    }

    public EventType getEventTypeSingle(EventAdapterService eventAdapterService, String statementId) throws ExprValidationException {
        if (sortedwin) {
            return null;
        }
        return containedType;
    }

    public EventBean evaluateGetEventBean(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context) {
        return super.aggregationResultFuture.getEventBean(column, eventsPerStream, isNewData, context);
    }

    public boolean isMax() {
        return max;
    }

    @Override
    protected boolean equalsNodeAggregateMethodOnly(ExprAggregateNode node) {
        return false;
    }

    private String getErrorPrefix() {
        return "The '" + getAggregationFunctionName() + "' aggregation function";
    }
}