/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.expression.table;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventType;
import com.espertech.esper.epl.agg.access.AggregationAccessor;
import com.espertech.esper.epl.agg.access.AggregationState;
import com.espertech.esper.epl.agg.service.AggregationMethodFactory;
import com.espertech.esper.epl.agg.service.AggregationRowPair;
import com.espertech.esper.epl.expression.accessagg.ExprAggregateAccessMultiValueNode;
import com.espertech.esper.epl.expression.core.*;
import com.espertech.esper.epl.table.mgmt.TableMetadataColumnAggregation;
import com.espertech.esper.epl.table.strategy.ExprTableEvalStrategyUtil;
import com.espertech.esper.event.EventAdapterService;
import com.espertech.esper.event.ObjectArrayBackedEventBean;
import com.espertech.esper.event.arr.ObjectArrayEventBean;

import java.io.StringWriter;
import java.util.Collection;

public class ExprTableIdentNodeSubpropAccessor extends ExprNodeBase implements ExprEvaluator, ExprEvaluatorEnumeration
{
    private static final long serialVersionUID = -8308528998078977774L;

    private final int streamNum;
    private final String optionalStreamName;
    private final TableMetadataColumnAggregation tableAccessColumn;
    private final ExprNode aggregateAccessMultiValueNode;
    private transient AggregationMethodFactory accessorFactory;
    private transient AggregationAccessor accessor;

    public ExprTableIdentNodeSubpropAccessor(int streamNum, String optionalStreamName, TableMetadataColumnAggregation tableAccessColumn, ExprNode aggregateAccessMultiValueNode) {
        this.streamNum = streamNum;
        this.optionalStreamName = optionalStreamName;
        this.tableAccessColumn = tableAccessColumn;
        this.aggregateAccessMultiValueNode = aggregateAccessMultiValueNode;
    }

    public ExprNode validate(ExprValidationContext validationContext) throws ExprValidationException {
        if (tableAccessColumn.getAccessAccessorSlotPair() == null) {
            throw new ExprValidationException("Invalid combination of aggregation state and aggregation accessor");
        }
        ExprAggregateAccessMultiValueNode mfNode = (ExprAggregateAccessMultiValueNode) aggregateAccessMultiValueNode;
        mfNode.validatePositionals();
        accessorFactory = mfNode.validateAggregationParamsWBinding(validationContext, tableAccessColumn);
        accessor = accessorFactory.getAccessor();
        return null;
    }

    public ExprEvaluator getExprEvaluator() {
        return this;
    }

    public Class getType() {
        return accessorFactory.getResultType();
    }

    public Object evaluate(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext exprEvaluatorContext) {
        AggregationState state = getState(eventsPerStream);
        if (state == null) {
            return null;
        }
        return accessor.getValue(state, eventsPerStream, isNewData, exprEvaluatorContext);
    }

    public EventType getEventTypeCollection(EventAdapterService eventAdapterService, String statementId) throws ExprValidationException {
        return ((ExprAggregateAccessMultiValueNode) aggregateAccessMultiValueNode).getEventTypeCollection(eventAdapterService, statementId);
    }

    public Collection<EventBean> evaluateGetROCollectionEvents(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context) {
        AggregationState state = getState(eventsPerStream);
        if (state == null) {
            return null;
        }
        return accessor.getEnumerableEvents(state, eventsPerStream, isNewData, context);
    }

    public Class getComponentTypeCollection() throws ExprValidationException {
        return ((ExprAggregateAccessMultiValueNode) aggregateAccessMultiValueNode).getComponentTypeCollection();
    }

    public Collection evaluateGetROCollectionScalar(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context) {
        AggregationState state = getState(eventsPerStream);
        if (state == null) {
            return null;
        }
        return accessor.getEnumerableScalar(state, eventsPerStream, isNewData, context);
    }

    public EventType getEventTypeSingle(EventAdapterService eventAdapterService, String statementId) throws ExprValidationException {
        return ((ExprAggregateAccessMultiValueNode) aggregateAccessMultiValueNode).getEventTypeSingle(eventAdapterService, statementId);
    }

    public EventBean evaluateGetEventBean(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context) {
        AggregationState state = getState(eventsPerStream);
        if (state == null) {
            return null;
        }
        return accessor.getEnumerableEvent(state, eventsPerStream, isNewData, context);
    }

    public void toPrecedenceFreeEPL(StringWriter writer) {
        if (optionalStreamName != null) {
            writer.append(optionalStreamName);
            writer.append(".");
        }
        writer.append(tableAccessColumn.getColumnName());
        writer.append(".");
        aggregateAccessMultiValueNode.toEPL(writer, ExprPrecedenceEnum.MINIMUM);
    }

    public ExprPrecedenceEnum getPrecedence() {
        return ExprPrecedenceEnum.UNARY;
    }

    public boolean isConstantResult() {
        return false;
    }

    public boolean equalsNode(ExprNode node) {
        return false;
    }

    private AggregationState getState(EventBean[] eventsPerStream) {
        EventBean event = eventsPerStream[streamNum];
        if (event == null) {
            return null;
        }
        AggregationRowPair row = ExprTableEvalStrategyUtil.getRow((ObjectArrayBackedEventBean) event);
        return row.getStates()[tableAccessColumn.getAccessAccessorSlotPair().getSlot()];
    }
}
