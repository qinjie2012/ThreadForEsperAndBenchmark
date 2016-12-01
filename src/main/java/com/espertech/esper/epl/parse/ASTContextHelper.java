/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.epl.parse;

import com.espertech.esper.epl.expression.core.ExprChainedSpec;
import com.espertech.esper.epl.expression.core.ExprNode;
import com.espertech.esper.epl.expression.time.ExprTimePeriod;
import com.espertech.esper.epl.generated.EsperEPL2GrammarParser;
import com.espertech.esper.epl.spec.*;
import com.espertech.esper.pattern.EvalFactoryNode;
import com.espertech.esper.util.JavaClassHelper;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.tree.Tree;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ASTContextHelper
{
    private static final Log log = LogFactory.getLog(ASTContextHelper.class);

    public static CreateContextDesc walkCreateContext(EsperEPL2GrammarParser.CreateContextExprContext ctx, Map<Tree, ExprNode> astExprNodeMap, Map<Tree, EvalFactoryNode> astPatternNodeMap, PropertyEvalSpec propertyEvalSpec, FilterSpecRaw filterSpec)
    {
        String contextName = ctx.name.getText();
        ContextDetail contextDetail;

        EsperEPL2GrammarParser.CreateContextChoiceContext choice = ctx.createContextDetail().createContextChoice();
        if (choice != null) {
            contextDetail = walkChoice(choice, astExprNodeMap, astPatternNodeMap, propertyEvalSpec, filterSpec);
        }
        else {
            contextDetail = walkNested(ctx.createContextDetail().contextContextNested(), astExprNodeMap, astPatternNodeMap, propertyEvalSpec, filterSpec);
        }
        return new CreateContextDesc(contextName, contextDetail);
    }

    private static ContextDetail walkNested(List<EsperEPL2GrammarParser.ContextContextNestedContext> nestedContexts, Map<Tree, ExprNode> astExprNodeMap, Map<Tree, EvalFactoryNode> astPatternNodeMap, PropertyEvalSpec propertyEvalSpec, FilterSpecRaw filterSpec) {
        List<CreateContextDesc> contexts = new ArrayList<CreateContextDesc>(nestedContexts.size());
        for (EsperEPL2GrammarParser.ContextContextNestedContext nestedctx : nestedContexts) {
            ContextDetail contextDetail = walkChoice(nestedctx.createContextChoice(), astExprNodeMap, astPatternNodeMap, propertyEvalSpec, filterSpec);
            CreateContextDesc desc = new CreateContextDesc(nestedctx.name.getText(), contextDetail);
            contexts.add(desc);
        }
        return new ContextDetailNested(contexts);
    }

    private static ContextDetail walkChoice(EsperEPL2GrammarParser.CreateContextChoiceContext ctx, Map<Tree, ExprNode> astExprNodeMap, Map<Tree, EvalFactoryNode> astPatternNodeMap, PropertyEvalSpec propertyEvalSpec, FilterSpecRaw filterSpec) {

        // temporal fixed (start+end) and overlapping (initiated/terminated)
        if (ctx.START() != null || ctx.INITIATED() != null) {

            ExprNode[] distinctExpressions = null;
            if (ctx.createContextDistinct() != null) {
                if (ctx.createContextDistinct().expressionList() == null) {
                    distinctExpressions = new ExprNode[0];
                }
                else {
                    distinctExpressions = ASTExprHelper.exprCollectSubNodesPerNode(ctx.createContextDistinct().expressionList().expression(), astExprNodeMap);
                }
            }

            ContextDetailCondition startEndpoint;
            if (ctx.START() != null) {
                boolean immediate = checkNow(ctx.i);
                if (immediate) {
                    startEndpoint = new ContextDetailConditionImmediate();
                }
                else {
                    startEndpoint = getContextCondition(ctx.r1, astExprNodeMap, astPatternNodeMap, propertyEvalSpec, false);
                }
            }
            else {
                boolean immediate = checkNow(ctx.i);
                startEndpoint = getContextCondition(ctx.r1, astExprNodeMap, astPatternNodeMap, propertyEvalSpec, immediate);
            }

            boolean overlapping = ctx.INITIATED() != null;
            ContextDetailCondition endEndpoint = getContextCondition(ctx.r2, astExprNodeMap, astPatternNodeMap, propertyEvalSpec, false);
            return new ContextDetailInitiatedTerminated(startEndpoint, endEndpoint, overlapping, distinctExpressions);
        }

        // partitioned
        if (ctx.PARTITION() != null){
            List<EsperEPL2GrammarParser.CreateContextPartitionItemContext> partitions = ctx.createContextPartitionItem();
            List<ContextDetailPartitionItem> rawSpecs = new ArrayList<ContextDetailPartitionItem>();
            for (EsperEPL2GrammarParser.CreateContextPartitionItemContext partition : partitions) {

                filterSpec = ASTFilterSpecHelper.walkFilterSpec(partition.eventFilterExpression(), propertyEvalSpec, astExprNodeMap);
                propertyEvalSpec = null;

                List<String> propertyNames = new ArrayList<String>();
                List<EsperEPL2GrammarParser.EventPropertyContext> properties = partition.eventProperty();
                for (EsperEPL2GrammarParser.EventPropertyContext property : properties) {
                    String propertyName = ASTUtil.getPropertyName(property, 0);
                    propertyNames.add(propertyName);
                }
                ASTExprHelper.exprCollectSubNodes(partition, 0, astExprNodeMap); // remove expressions

                rawSpecs.add(new ContextDetailPartitionItem(filterSpec, propertyNames));
            }
            return new ContextDetailPartitioned(rawSpecs);
        }

        // hash
        else if (ctx.COALESCE() != null){
            List<EsperEPL2GrammarParser.CreateContextCoalesceItemContext> coalesces = ctx.createContextCoalesceItem();
            List<ContextDetailHashItem> rawSpecs = new ArrayList<ContextDetailHashItem>(coalesces.size());
            for (EsperEPL2GrammarParser.CreateContextCoalesceItemContext coalesce : coalesces) {
                ExprChainedSpec func = ASTLibFunctionHelper.getLibFunctionChainSpec(coalesce.libFunctionNoClass(), astExprNodeMap);
                filterSpec = ASTFilterSpecHelper.walkFilterSpec(coalesce.eventFilterExpression(), propertyEvalSpec, astExprNodeMap);
                propertyEvalSpec = null;
                rawSpecs.add(new ContextDetailHashItem(func, filterSpec));
            }

            String granularity = ctx.g.getText();
            if (!granularity.toLowerCase().equals("granularity")) {
                throw ASTWalkException.from("Expected 'granularity' keyword after list of coalesce items, found '" + granularity + "' instead");
            }
            Number num = (Number) ASTConstantHelper.parse(ctx.number());
            String preallocateStr = ctx.p != null ? ctx.p.getText() : null;
            if (preallocateStr != null && !preallocateStr.toLowerCase().equals("preallocate")) {
                throw ASTWalkException.from("Expected 'preallocate' keyword after list of coalesce items, found '" + preallocateStr + "' instead");
            }
            if (!JavaClassHelper.isNumericNonFP(num.getClass()) || JavaClassHelper.getBoxedType(num.getClass()) == Long.class) {
                throw ASTWalkException.from("Granularity provided must be an int-type number, received " + num.getClass() + " instead");
            }

            return new ContextDetailHash(rawSpecs, num.intValue(), preallocateStr != null);
        }

        // categorized
        if (ctx.createContextGroupItem() != null){
            List<EsperEPL2GrammarParser.CreateContextGroupItemContext> grps = ctx.createContextGroupItem();
            List<ContextDetailCategoryItem> items = new ArrayList<ContextDetailCategoryItem>();
            for (EsperEPL2GrammarParser.CreateContextGroupItemContext grp : grps) {
                ExprNode exprNode = ASTExprHelper.exprCollectSubNodes(grp, 0, astExprNodeMap).get(0);
                String name = grp.i.getText();
                items.add(new ContextDetailCategoryItem(exprNode, name));
            }
            filterSpec = ASTFilterSpecHelper.walkFilterSpec(ctx.eventFilterExpression(), propertyEvalSpec, astExprNodeMap);
            return new ContextDetailCategory(items, filterSpec);
        }

        throw new IllegalStateException("Unrecognized context detail type");
    }

    private static ContextDetailCondition getContextCondition(EsperEPL2GrammarParser.CreateContextRangePointContext ctx, Map<Tree, ExprNode> astExprNodeMap, Map<Tree, EvalFactoryNode> astPatternNodeMap, PropertyEvalSpec propertyEvalSpec, boolean immediate) {
        if (ctx.crontabLimitParameterSet() != null) {
            List<ExprNode> crontab = ASTExprHelper.exprCollectSubNodes(ctx.crontabLimitParameterSet(), 0, astExprNodeMap);
            return new ContextDetailConditionCrontab(crontab, immediate);
        }
        else if (ctx.patternInclusionExpression() != null) {
            EvalFactoryNode evalNode = ASTExprHelper.patternGetRemoveTopNode(ctx.patternInclusionExpression(), astPatternNodeMap);
            boolean inclusive = false;
            if (ctx.i != null) {
                String ident = ctx.i.getText();
                if (ident != null && !ident.toLowerCase().equals("inclusive")) {
                    throw ASTWalkException.from("Expected 'inclusive' keyword after '@', found '" + ident + "' instead");
                }
                inclusive = true;
            }
            return new ContextDetailConditionPattern(evalNode, inclusive, immediate);
        }
        else if (ctx.createContextFilter() != null) {
            FilterSpecRaw filterSpecRaw = ASTFilterSpecHelper.walkFilterSpec(ctx.createContextFilter().eventFilterExpression(), propertyEvalSpec, astExprNodeMap);
            String asName = ctx.createContextFilter().i != null ? ctx.createContextFilter().i.getText() : null;
            if (immediate) {
                throw ASTWalkException.from("Invalid use of 'now' with initiated-by stream, this combination is not supported");
            }
            return new ContextDetailConditionFilter(filterSpecRaw, asName);
        }
        else if (ctx.AFTER() != null) {
            ExprTimePeriod timePeriod = (ExprTimePeriod) ASTExprHelper.exprCollectSubNodes(ctx.timePeriod(), 0, astExprNodeMap).get(0);
            return new ContextDetailConditionTimePeriod(timePeriod, immediate);
        }
        else {
            throw new IllegalStateException("Unrecognized child type");
        }
    }

    private static boolean checkNow(Token i) {
        if (i == null) {
            return false;
        }
        String ident = i.getText();
        if (!ident.toLowerCase().equals("now")) {
            throw ASTWalkException.from("Expected 'now' keyword after '@', found '" + ident + "' instead");
        }
        return true;
    }
}
