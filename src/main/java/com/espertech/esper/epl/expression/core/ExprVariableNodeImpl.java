/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.expression.core;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.EventPropertyGetter;
import com.espertech.esper.client.EventType;
import com.espertech.esper.epl.core.DuplicatePropertyException;
import com.espertech.esper.epl.core.PropertyNotFoundException;
import com.espertech.esper.epl.variable.VariableMetaData;
import com.espertech.esper.epl.variable.VariableReader;
import com.espertech.esper.epl.variable.VariableService;
import com.espertech.esper.event.EventTypeSPI;
import com.espertech.esper.metrics.instrumentation.InstrumentationHelper;

import java.io.StringWriter;
import java.util.Map;

/**
 * Represents a variable in an expression tree.
 */
public class ExprVariableNodeImpl extends ExprNodeBase implements ExprEvaluator, ExprVariableNode
{
    private static final long serialVersionUID = 0L;

    private final String variableName;
    private final String optSubPropName;
    private final boolean isConstant;
    private final Object valueIfConstant;

    private Class variableType;
    private boolean isPrimitive;

    private transient EventPropertyGetter eventTypeGetter;
    private transient Map<Integer, VariableReader> readersPerCp;
    private transient VariableReader readerNonCP;

    /**
     * Ctor.
     */
    public ExprVariableNodeImpl(VariableMetaData variableMetaData, String optSubPropName)
    {
        if (variableMetaData == null) {
            throw new IllegalArgumentException("Variables metadata is null");
        }
        this.variableName = variableMetaData.getVariableName();
        this.optSubPropName = optSubPropName;
        this.isConstant = variableMetaData.isConstant();
        this.valueIfConstant = isConstant ? variableMetaData.getVariableStateFactory().getInitialState() : null;
    }

    public boolean isConstantValue() {
        return isConstant;
    }

    public ExprEvaluator getExprEvaluator() {
        return this;
    }

    public String getVariableName() {
        return variableName;
    }

    public Object getConstantValue(ExprEvaluatorContext context) {
        if (isConstant) {
            return valueIfConstant;
        }
        return null;
    }

    public boolean isConstantResult() {
        return isConstant;
    }

    public ExprNode validate(ExprValidationContext validationContext) throws ExprValidationException
    {
        // determine if any types are property agnostic; If yes, resolve to variable
        boolean hasPropertyAgnosticType = false;
        EventType[] types = validationContext.getStreamTypeService().getEventTypes();
        for (int i = 0; i < validationContext.getStreamTypeService().getEventTypes().length; i++)
        {
            if (types[i] instanceof EventTypeSPI)
            {
                hasPropertyAgnosticType |= ((EventTypeSPI) types[i]).getMetadata().isPropertyAgnostic();
            }
        }

        if (!hasPropertyAgnosticType)
        {
            // the variable name should not overlap with a property name
            try
            {
                validationContext.getStreamTypeService().resolveByPropertyName(variableName, false);
                throw new ExprValidationException("The variable by name '" + variableName + "' is ambigous to a property of the same name");
            }
            catch (DuplicatePropertyException e)
            {
                throw new ExprValidationException("The variable by name '" + variableName + "' is ambigous to a property of the same name");
            }
            catch (PropertyNotFoundException e)
            {
                // expected
            }
        }

        VariableMetaData variableMetadata = validationContext.getVariableService().getVariableMetaData(variableName);
        if (variableMetadata == null) {
            throw new ExprValidationException("Failed to find variable by name '" + variableName + "'");
        }
        isPrimitive = variableMetadata.getEventType() == null;
        variableType = variableMetadata.getType();
        if (optSubPropName != null) {
            if (variableMetadata.getEventType() == null) {
                throw new ExprValidationException("Property '" + optSubPropName + "' is not valid for variable '" + variableName + "'");
            }
            eventTypeGetter = variableMetadata.getEventType().getGetter(optSubPropName);
            if (eventTypeGetter == null) {
                throw new ExprValidationException("Property '" + optSubPropName + "' is not valid for variable '" + variableName + "'");
            }
            variableType = variableMetadata.getEventType().getPropertyType(optSubPropName);
        }

        readersPerCp = validationContext.getVariableService().getReadersPerCP(variableName);
        if (variableMetadata.getContextPartitionName() == null) {
            readerNonCP = readersPerCp.get(VariableService.NOCONTEXT_AGENTINSTANCEID);
        }
        return null;
    }

    public Class getConstantType()
    {
        return variableType;
    }

    public Class getType()
    {
        return variableType;
    }

    public String toString()
    {
        return "variableName=" + variableName;
    }

    public Object evaluate(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext exprEvaluatorContext)
    {
        VariableReader reader;
        if (readerNonCP != null) {
            reader = readerNonCP;
        }
        else {
            reader = readersPerCp.get(exprEvaluatorContext.getAgentInstanceId());
        }

        if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().qExprVariable(this);}
        Object value = reader.getValue();
        if (isPrimitive || value == null) {
            if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().aExprVariable(value);}
            return value;
        }

        EventBean theEvent = (EventBean) value;
        if (optSubPropName == null) {
            if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().aExprVariable(theEvent.getUnderlying());}
            return theEvent.getUnderlying();
        }
        Object result = eventTypeGetter.get(theEvent);
        if (InstrumentationHelper.ENABLED) { InstrumentationHelper.get().aExprVariable(result);}
        return result;
    }

    public void toPrecedenceFreeEPL(StringWriter writer) {
        writer.append(variableName);
        if (optSubPropName != null) {
            writer.append(".");
            writer.append(optSubPropName);
        }
    }

    public ExprPrecedenceEnum getPrecedence() {
        return ExprPrecedenceEnum.UNARY;
    }

    public boolean equalsNode(ExprNode node)
    {
        if (!(node instanceof ExprVariableNodeImpl))
        {
            return false;
        }

        ExprVariableNodeImpl that = (ExprVariableNodeImpl) node;

        if (optSubPropName != null ? !optSubPropName.equals(that.optSubPropName) : that.optSubPropName != null) {
            return false;
        }
        return that.variableName.equals(this.variableName);
    }

    public String getVariableNameWithSubProp() {
        if (optSubPropName == null) {
            return variableName;
        }
        return variableName + "." + optSubPropName;
    }
}
