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
import com.espertech.esper.client.util.CountMinSketchAgent;
import com.espertech.esper.client.util.CountMinSketchAgentStringUTF16;
import com.espertech.esper.core.service.StatementType;
import com.espertech.esper.epl.agg.service.AggregationMethodFactory;
import com.espertech.esper.epl.approx.CountMinSketchAggStateFactory;
import com.espertech.esper.epl.approx.CountMinSketchAggType;
import com.espertech.esper.epl.approx.CountMinSketchSpec;
import com.espertech.esper.epl.approx.CountMinSketchSpecHashes;
import com.espertech.esper.epl.core.EngineImportService;
import com.espertech.esper.epl.expression.baseagg.ExprAggregateNode;
import com.espertech.esper.epl.expression.baseagg.ExprAggregateNodeBase;
import com.espertech.esper.epl.expression.core.*;
import com.espertech.esper.epl.table.mgmt.TableMetadataColumnAggregation;
import com.espertech.esper.event.EventAdapterService;
import com.espertech.esper.util.*;

import java.util.Collection;
import java.util.Map;

/**
 * Represents the Count-min sketch aggregate function.
 */
public class ExprAggCountMinSketchNode extends ExprAggregateNodeBase implements ExprAggregateAccessMultiValueNode
{
    private static final long serialVersionUID = 202339518989532184L;

    private static final double DEFAULT__EPS_OF_TOTAL_COUNT = 0.0001;
    private static final double DEFAULT__CONFIDENCE = 0.99;
    private static final int DEFAULT__SEED = 1234567;
    private static final CountMinSketchAgentStringUTF16 DEFAULT__AGENT = new CountMinSketchAgentStringUTF16();

    private static final String MSG_NAME = "Count-min-sketch";
    private static final String NAME__EPS_OF_TOTAL_COUNT = "epsOfTotalCount";
    private static final String NAME__CONFIDENCE = "confidence";
    private static final String NAME__SEED = "seed";
    private static final String NAME__TOPK = "topk";
    private static final String NAME__AGENT = "agent";

    private final CountMinSketchAggType aggType;

    /**
     * Ctor.
     * @param distinct - flag indicating unique or non-unique value aggregation
     */
    public ExprAggCountMinSketchNode(boolean distinct, CountMinSketchAggType aggType) {
        super(distinct);
        this.aggType = aggType;
    }

    public AggregationMethodFactory validateAggregationChild(ExprValidationContext validationContext) throws ExprValidationException {
        return validateAggregationInternal(validationContext, null);
    }

    public AggregationMethodFactory validateAggregationParamsWBinding(ExprValidationContext context, TableMetadataColumnAggregation tableAccessColumn) throws ExprValidationException {
        return validateAggregationInternal(context, tableAccessColumn);
    }

    public String getAggregationFunctionName()
    {
        return aggType.getFuncName();
    }

    public final boolean equalsNodeAggregateMethodOnly(ExprAggregateNode node)
    {
        return false;
    }

    public CountMinSketchAggType getAggType() {
        return aggType;
    }

    public EventType getEventTypeCollection(EventAdapterService eventAdapterService, String statementId) throws ExprValidationException {
        return null;
    }

    public Collection<EventBean> evaluateGetROCollectionEvents(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context) {
        return null;
    }

    public Class getComponentTypeCollection() throws ExprValidationException {
        return null;
    }

    public Collection evaluateGetROCollectionScalar(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context) {
        return null;
    }

    public EventType getEventTypeSingle(EventAdapterService eventAdapterService, String statementId) throws ExprValidationException {
        return null;
    }

    public EventBean evaluateGetEventBean(EventBean[] eventsPerStream, boolean isNewData, ExprEvaluatorContext context) {
        return null;
    }

    @Override
    protected boolean isExprTextWildcardWhenNoParams() {
        return false;
    }

    private AggregationMethodFactory validateAggregationInternal(ExprValidationContext context, TableMetadataColumnAggregation tableAccessColumn)
            throws ExprValidationException
    {
        if (isDistinct()) {
            throw new ExprValidationException(getMessagePrefix() + "is not supported with distinct");
        }

        // for declaration, validate the specification and return the state factory
        if (aggType == CountMinSketchAggType.STATE) {
            if (context.getExprEvaluatorContext().getStatementType() != StatementType.CREATE_TABLE) {
                throw new ExprValidationException(getMessagePrefix() + "can only be used in create-table statements");
            }
            CountMinSketchSpec specification = validateSpecification(context.getExprEvaluatorContext(), context.getMethodResolutionService().getEngineImportService());
            return new ExprAggCountMinSketchNodeFactoryState(new CountMinSketchAggStateFactory(this, specification));
        }

        // validate number of parameters
        if (aggType == CountMinSketchAggType.ADD || aggType == CountMinSketchAggType.FREQ) {
            if (this.getChildNodes().length == 0 || this.getChildNodes().length > 1) {
                throw new ExprValidationException(getMessagePrefix() + "requires a single parameter expression");
            }
        }
        else {
            if (this.getChildNodes().length != 0) {
                throw new ExprValidationException(getMessagePrefix() + "requires a no parameter expressions");
            }
        }

        // validate into-table and table-access
        if (aggType == CountMinSketchAggType.ADD) {
            if (context.getIntoTableName() == null) {
                throw new ExprValidationException(getMessagePrefix() + "can only be used with into-table");
            }
        }
        else {
            if (tableAccessColumn == null) {
                throw new ExprValidationException(getMessagePrefix() + "requires the use of a table-access expression");
            }
            ExprNodeUtility.getValidatedSubtree(ExprNodeOrigin.AGGPARAM, this.getChildNodes(), context);
        }

        // obtain evaluator
        ExprEvaluator addOrFrequencyEvaluator = null;
        if (aggType == CountMinSketchAggType.ADD || aggType == CountMinSketchAggType.FREQ) {
            addOrFrequencyEvaluator = getChildNodes()[0].getExprEvaluator();
        }

        return new ExprAggCountMinSketchNodeFactoryUse(this, addOrFrequencyEvaluator);
    }

    private CountMinSketchSpec validateSpecification(ExprEvaluatorContext exprEvaluatorContext, final EngineImportService engineImportService) throws ExprValidationException {
        // default specification
        final CountMinSketchSpec spec = new CountMinSketchSpec(new CountMinSketchSpecHashes(DEFAULT__EPS_OF_TOTAL_COUNT, DEFAULT__CONFIDENCE, DEFAULT__SEED), null, DEFAULT__AGENT);

        // no parameters
        if (this.getChildNodes().length == 0) {
            return spec;
        }

        // check expected parameter type: a json object
        if (this.getChildNodes().length > 1 || !(this.getChildNodes()[0] instanceof ExprConstantNode)) {
            throw getDeclaredWrongParameterExpr();
        }
        ExprConstantNode constantNode = (ExprConstantNode) this.getChildNodes()[0];
        Object value = constantNode.getConstantValue(exprEvaluatorContext);
        if (!(value instanceof Map)) {
            throw getDeclaredWrongParameterExpr();
        }

        // define what to populate
        PopulateFieldWValueDescriptor[] descriptors = new PopulateFieldWValueDescriptor[] {
            new PopulateFieldWValueDescriptor(NAME__EPS_OF_TOTAL_COUNT, Double.class, spec.getHashesSpec().getClass(), new PopulateFieldValueSetter() {
                public void set(Object value) {
                    if (value != null) {spec.getHashesSpec().setEpsOfTotalCount((Double) value);}
                }
            }, true),
            new PopulateFieldWValueDescriptor(NAME__CONFIDENCE, Double.class, spec.getHashesSpec().getClass(), new PopulateFieldValueSetter() {
                public void set(Object value) {
                    if (value != null) {spec.getHashesSpec().setConfidence((Double) value);}
                }
            }, true),
            new PopulateFieldWValueDescriptor(NAME__SEED, Integer.class, spec.getHashesSpec().getClass(), new PopulateFieldValueSetter() {
                public void set(Object value) {
                    if (value != null) {spec.getHashesSpec().setSeed((Integer) value);}
                }
            }, true),
            new PopulateFieldWValueDescriptor(NAME__TOPK, Integer.class, spec.getClass(), new PopulateFieldValueSetter() {
                public void set(Object value) {
                    if (value != null) {spec.setTopkSpec((Integer) value);}
                }
            }, true),
            new PopulateFieldWValueDescriptor(NAME__AGENT, String.class, spec.getClass(), new PopulateFieldValueSetter() {
                public void set(Object value) throws ExprValidationException {
                    if (value != null) {
                        CountMinSketchAgent transform;
                        try {
                            Class transformClass = engineImportService.resolveClass((String) value);
                            transform = (CountMinSketchAgent) JavaClassHelper.instantiate(CountMinSketchAgent.class, transformClass.getName());
                        }
                        catch (Exception e) {
                            throw new ExprValidationException("Failed to instantiate agent provider: " + e.getMessage(), e);
                        }
                        spec.setAgent(transform);
                    }
                }
            }, true),
        };

        // populate from json, validates incorrect names, coerces types, instantiates transform
        PopulateUtil.populateSpecCheckParameters(descriptors, (Map<String, Object>) value, spec, engineImportService);

        return spec;
    }


    public ExprValidationException getDeclaredWrongParameterExpr() throws ExprValidationException {
        return new ExprValidationException(getMessagePrefix() + " expects either no parameter or a single json parameter object");
    }

    private String getMessagePrefix() {
        return MSG_NAME + " aggregation function '" + aggType.getFuncName() + "' ";
    }
}