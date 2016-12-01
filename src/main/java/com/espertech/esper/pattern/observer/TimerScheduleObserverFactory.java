/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.pattern.observer;

import com.espertech.esper.client.EPException;
import com.espertech.esper.client.util.TimePeriod;
import com.espertech.esper.epl.expression.core.*;
import com.espertech.esper.epl.expression.time.ExprTimePeriod;
import com.espertech.esper.pattern.*;
import com.espertech.esper.schedule.ScheduleParameterException;
import com.espertech.esper.util.MetaDefItem;

import java.io.Serializable;
import java.util.*;

/**
 * Factory for ISO8601 repeating interval observers that indicate truth when a time point was reached.
 */
public class TimerScheduleObserverFactory implements ObserverFactory, MetaDefItem, Serializable
{
    private final static String NAME_OBSERVER = "Timer-schedule observer";
    
    private final static String ISO_NAME = "iso";
    private final static String REPETITIONS_NAME = "repetitions";
    private final static String DATE_NAME = "date";
    private final static String PERIOD_NAME = "period";
    private final static String[] NAMED_PARAMETERS = {ISO_NAME, REPETITIONS_NAME, DATE_NAME, PERIOD_NAME};
    private static final long serialVersionUID = 6180792299082096772L;

    /**
     * Convertor.
     */
    protected transient TimerScheduleSpecCompute scheduleComputer;
    protected transient MatchedEventConvertor convertor;

    protected TimerScheduleSpec spec;

    public void setObserverParameters(List<ExprNode> parameters, MatchedEventConvertor convertor, ExprValidationContext validationContext) throws ObserverParameterException
    {
        this.convertor = convertor;

        // obtains name parameters
        Map<String, ExprNamedParameterNode> namedExpressions;
        try {
            namedExpressions = ExprNodeUtility.getNamedExpressionsHandleDups(parameters);
            ExprNodeUtility.validateNamed(namedExpressions, NAMED_PARAMETERS);
        }
        catch (ExprValidationException e) {
            throw new ObserverParameterException(e.getMessage(), e);
        }

        boolean allConstantResult;
        ExprNamedParameterNode isoStringExpr = namedExpressions.get(ISO_NAME);
        if (namedExpressions.size() == 1 && isoStringExpr != null) {
            try {
                allConstantResult = ExprNodeUtility.validateNamedExpectType(isoStringExpr, new Class[] {String.class});
            }
            catch (ExprValidationException ex) {
                throw new ObserverParameterException(ex.getMessage(), ex);
            }
            scheduleComputer = new TimerScheduleSpecComputeISOString(isoStringExpr.getChildNodes()[0]);
        }
        else if (isoStringExpr != null) {
            throw new ObserverParameterException("The '" + ISO_NAME + "' parameter is exclusive of other parameters");
        }
        else if (namedExpressions.size() == 0) {
            throw new ObserverParameterException("No parameters provided");
        }
        else {
            allConstantResult = true;
            ExprNamedParameterNode dateNamedNode = namedExpressions.get(DATE_NAME);
            ExprNamedParameterNode repetitionsNamedNode = namedExpressions.get(REPETITIONS_NAME);
            ExprNamedParameterNode periodNamedNode = namedExpressions.get(PERIOD_NAME);
            if (dateNamedNode == null && periodNamedNode == null) {
                throw new ObserverParameterException("Either the date or period parameter is required");
            }
            try {
                if (dateNamedNode != null) {
                    allConstantResult = ExprNodeUtility.validateNamedExpectType(dateNamedNode, new Class[] {String.class, Calendar.class, Date.class, Long.class});
                }
                if (repetitionsNamedNode != null) {
                    allConstantResult &= ExprNodeUtility.validateNamedExpectType(repetitionsNamedNode, new Class[] {Integer.class, Long.class});
                }
                if (periodNamedNode != null) {
                    allConstantResult &= ExprNodeUtility.validateNamedExpectType(periodNamedNode, new Class[] {TimePeriod.class});
                }
            }
            catch (ExprValidationException ex) {
                throw new ObserverParameterException(ex.getMessage(), ex);
            }
            ExprNode dateNode = dateNamedNode == null ? null : dateNamedNode.getChildNodes()[0];
            ExprNode repetitionsNode = repetitionsNamedNode == null ? null : repetitionsNamedNode.getChildNodes()[0];
            ExprTimePeriod periodNode = periodNamedNode == null ? null : (ExprTimePeriod) periodNamedNode.getChildNodes()[0];
            scheduleComputer = new TimerScheduleSpecComputeFromExpr(dateNode, repetitionsNode, periodNode);
        }

        if (allConstantResult) {
            try {
                spec = scheduleComputer.compute(convertor, new MatchedEventMapImpl(convertor.getMatchedEventMapMeta()), null, validationContext.getMethodResolutionService().getEngineImportService().getTimeZone());
            }
            catch (ScheduleParameterException ex) {
                throw new ObserverParameterException(ex.getMessage(), ex);
            }
        }
    }

    public EventObserver makeObserver(PatternAgentInstanceContext context, MatchedEventMap beginState, ObserverEventEvaluator observerEventEvaluator,
                                      EvalStateNodeNumber stateNodeId, Object observerState, boolean isFilterChildNonQuitting) {
        return new TimerScheduleObserver(computeSpecDynamic(beginState, context), beginState, observerEventEvaluator, isFilterChildNonQuitting);
    }

    public boolean isNonRestarting() {
        return true;
    }

    public TimerScheduleSpec computeSpecDynamic(MatchedEventMap beginState, PatternAgentInstanceContext context) {
        if (spec != null) {
            return spec;
        }
        try {
            return scheduleComputer.compute(convertor, beginState, context.getAgentInstanceContext(), context.getStatementContext().getMethodResolutionService().getEngineImportService().getTimeZone());
        }
        catch (ScheduleParameterException e) {
            throw new EPException("Error computing iso8601 schedule specification: " + e.getMessage(), e);
        }
    }

    private static interface TimerScheduleSpecCompute {
        public TimerScheduleSpec compute(MatchedEventConvertor convertor, MatchedEventMap beginState, ExprEvaluatorContext exprEvaluatorContext, TimeZone timeZone)
                throws ScheduleParameterException;
    }

    private static class TimerScheduleSpecComputeISOString implements TimerScheduleSpecCompute {
        private final ExprNode parameter;

        private TimerScheduleSpecComputeISOString(ExprNode parameter) {
            this.parameter = parameter;
        }

        public TimerScheduleSpec compute(MatchedEventConvertor convertor, MatchedEventMap beginState, ExprEvaluatorContext exprEvaluatorContext, TimeZone timeZone) throws ScheduleParameterException {
            Object param = PatternExpressionUtil.evaluate(NAME_OBSERVER, beginState, parameter, convertor, exprEvaluatorContext);
            String iso = (String) param;
            if (iso == null) {
                throw new ScheduleParameterException("Received null parameter value");
            }
            return TimerScheduleISO8601Parser.parse(iso);
        }
    }

    private static class TimerScheduleSpecComputeFromExpr implements TimerScheduleSpecCompute {
        private final ExprNode dateNode;
        private final ExprNode repetitionsNode;
        private final ExprTimePeriod periodNode;

        private TimerScheduleSpecComputeFromExpr(ExprNode dateNode, ExprNode repetitionsNode, ExprTimePeriod periodNode) {
            this.dateNode = dateNode;
            this.repetitionsNode = repetitionsNode;
            this.periodNode = periodNode;
        }

        public TimerScheduleSpec compute(MatchedEventConvertor convertor, MatchedEventMap beginState, ExprEvaluatorContext exprEvaluatorContext, TimeZone timeZone) throws ScheduleParameterException {
            Calendar optionalDate = null;
            if (dateNode != null) {
                Object param = PatternExpressionUtil.evaluate(NAME_OBSERVER, beginState, dateNode, convertor, exprEvaluatorContext);
                if (param instanceof String) {
                    optionalDate = TimerScheduleISO8601Parser.parseDate((String) param);
                }
                if (param instanceof Number) {
                    long msec = ((Number) param).longValue();
                    optionalDate = Calendar.getInstance(timeZone);
                    optionalDate.setTimeInMillis(msec);
                }
                if (param instanceof Calendar) {
                    optionalDate = (Calendar) param;
                }
                if (param instanceof Date) {
                    optionalDate = Calendar.getInstance(timeZone);
                    optionalDate.setTimeInMillis(((Date) param).getTime());
                }
            }

            TimePeriod optionalTimePeriod = null;
            if (periodNode != null) {
                Object param = PatternExpressionUtil.evaluateTimePeriod(NAME_OBSERVER, beginState, periodNode, convertor, exprEvaluatorContext);
                optionalTimePeriod = (TimePeriod) param;
            }

            Long optionalRepeatCount = null;
            if (repetitionsNode != null) {
                Object param = PatternExpressionUtil.evaluate(NAME_OBSERVER, beginState, repetitionsNode, convertor, exprEvaluatorContext);
                if (param != null) {
                    optionalRepeatCount = ((Number) param).longValue();
                }
            }
            
            if (optionalDate == null && optionalTimePeriod == null) {
                throw new EPException("Required date or time period are both null for " + NAME_OBSERVER);
            }

            return new TimerScheduleSpec(optionalDate, optionalRepeatCount, optionalTimePeriod);
        }
    }
}
