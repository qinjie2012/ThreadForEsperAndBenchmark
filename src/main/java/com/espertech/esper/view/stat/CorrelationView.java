/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.view.stat;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventType;
import com.espertech.esper.core.context.util.AgentInstanceContext;
import com.espertech.esper.core.service.StatementContext;
import com.espertech.esper.epl.expression.core.ExprNode;
import com.espertech.esper.event.EventAdapterService;
import com.espertech.esper.view.CloneableView;
import com.espertech.esper.view.View;
import com.espertech.esper.view.ViewFactory;
import com.espertech.esper.view.ViewFieldEnum;

import java.util.HashMap;
import java.util.Map;

/**
 * A view that calculates correlation on two fields. The view uses internally a {@link BaseStatisticsBean}
 * instance for the calculations, it also returns this bean as the result.
 * This class accepts most of its behaviour from its parent, {@link com.espertech.esper.view.stat.BaseBivariateStatisticsView}. It adds
 * the usage of the correlation bean and the appropriate schema.
 */
public class CorrelationView extends BaseBivariateStatisticsView implements CloneableView
{
    /**
     * Constructor.
     * @param xExpression is the expression providing X data points
     * @param yExpression is the expression providing X data points
     * @param agentInstanceContext contains required view services
     * @param eventType event type
     * @param additionalProps additional properties
     */
    public CorrelationView(ViewFactory viewFactory, AgentInstanceContext agentInstanceContext, ExprNode xExpression, ExprNode yExpression, EventType eventType, StatViewAdditionalProps additionalProps)
    {
        super(viewFactory, agentInstanceContext, xExpression, yExpression, eventType, additionalProps);
    }

    public View cloneView()
    {
        return new CorrelationView(viewFactory, agentInstanceContext, this.getExpressionX(), this.getExpressionY(), eventType, additionalProps);
    }

    public EventBean populateMap(BaseStatisticsBean baseStatisticsBean,
                                         EventAdapterService eventAdapterService,
                                         EventType eventType,
                                         StatViewAdditionalProps additionalProps,
                                         Object[] decoration)
    {
        return doPopulateMap(baseStatisticsBean,eventAdapterService,eventType,additionalProps,decoration);
    }

    /**
     * Populate bean.
     * @param baseStatisticsBean results
     * @param eventAdapterService event wrapping
     * @param eventType type to produce
     * @param additionalProps addition properties
     * @param decoration decoration values
     * @return bean
     */
    public static EventBean doPopulateMap(BaseStatisticsBean baseStatisticsBean,
                                         EventAdapterService eventAdapterService,
                                         EventType eventType,
                                         StatViewAdditionalProps additionalProps,
                                         Object[] decoration)
    {
        Map<String, Object> result = new HashMap<String, Object>();
        result.put(ViewFieldEnum.CORRELATION__CORRELATION.getName(), baseStatisticsBean.getCorrelation());
        if (additionalProps != null) {
            additionalProps.addProperties(result, decoration);
        }
        return eventAdapterService.adapterForTypedMap(result, eventType);
    }

    public EventType getEventType()
    {
        return eventType;
    }

    public String toString()
    {
        return this.getClass().getName() +
                " fieldX=" + this.getExpressionX() +
                " fieldY=" + this.getExpressionY();
    }

    /**
     * Creates the event type for this view.
     * @param statementContext is the event adapter service
     * @param additionalProps additional props
     * @return event type of view
     */
    protected static EventType createEventType(StatementContext statementContext, StatViewAdditionalProps additionalProps, int streamNum)
    {
        Map<String, Object> eventTypeMap = new HashMap<String, Object>();
        eventTypeMap.put(ViewFieldEnum.CORRELATION__CORRELATION.getName(), Double.class);
        StatViewAdditionalProps.addCheckDupProperties(eventTypeMap, additionalProps,
                ViewFieldEnum.CORRELATION__CORRELATION);
        String outputEventTypeName = statementContext.getStatementId() + "_correlview_" + streamNum;
        return statementContext.getEventAdapterService().createAnonymousMapType(outputEventTypeName, eventTypeMap);
    }
}
