/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.view.stat;

import com.espertech.esper.client.EventType;
import com.espertech.esper.core.context.util.AgentInstanceViewFactoryChainContext;
import com.espertech.esper.core.service.StatementContext;
import com.espertech.esper.epl.expression.core.ExprNode;
import com.espertech.esper.epl.expression.core.ExprNodeUtility;
import com.espertech.esper.util.JavaClassHelper;
import com.espertech.esper.view.*;

import java.util.List;

/**
 * Factory for {@link UnivariateStatisticsView} instances.
 */
public class UnivariateStatisticsViewFactory implements ViewFactory
{
    protected final static String NAME = "Univariate statistics";

    private List<ExprNode> viewParameters;
    private int streamNumber;

    /**
     * Property name of data field.
     */
    protected ExprNode fieldExpression;
    protected StatViewAdditionalProps additionalProps;

    protected EventType eventType;

    public void setViewParameters(ViewFactoryContext viewFactoryContext, List<ExprNode> expressionParameters) throws ViewParameterException
    {
        this.viewParameters = expressionParameters;
        this.streamNumber = viewFactoryContext.getStreamNum();
    }

    public void attach(EventType parentEventType, StatementContext statementContext, ViewFactory optionalParentFactory, List<ViewFactory> parentViewFactories) throws ViewParameterException
    {
        ExprNode[] validated = ViewFactorySupport.validate(getViewName(), parentEventType, statementContext, viewParameters, true);
        if (validated.length < 1) {
            throw new ViewParameterException(getViewParamMessage());
        }
        if (!JavaClassHelper.isNumeric(validated[0].getExprEvaluator().getType()))
        {
            throw new ViewParameterException(getViewParamMessage());
        }
        fieldExpression = validated[0];

        additionalProps = StatViewAdditionalProps.make(validated, 1, parentEventType);
        eventType = UnivariateStatisticsView.createEventType(statementContext, additionalProps, streamNumber);
    }

    public View makeView(AgentInstanceViewFactoryChainContext agentInstanceViewFactoryContext)
    {
        return new UnivariateStatisticsView(this, agentInstanceViewFactoryContext);
    }

    public EventType getEventType()
    {
        return eventType;
    }

    public boolean canReuse(View view)
    {
        if (!(view instanceof UnivariateStatisticsView)) {
            return false;
        }
        if (additionalProps != null) {
            return false;
        }

        UnivariateStatisticsView other = (UnivariateStatisticsView) view;
        if (!ExprNodeUtility.deepEquals(other.getFieldExpression(), fieldExpression))
        {
            return false;
        }

        return true;
    }

    public String getViewName() {
        return NAME;
    }

    private String getViewParamMessage() {
        return getViewName() + " view require a single expression returning a numeric value as a parameter";
    }

    public void setFieldExpression(ExprNode fieldExpression) {
        this.fieldExpression = fieldExpression;
    }

    public void setAdditionalProps(StatViewAdditionalProps additionalProps) {
        this.additionalProps = additionalProps;
    }

    public StatViewAdditionalProps getAdditionalProps() {
        return additionalProps;
    }

    public void setEventType(EventType eventType) {
        this.eventType = eventType;
    }
}
