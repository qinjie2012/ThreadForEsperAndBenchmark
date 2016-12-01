/**************************************************************************************
 * Copyright (C) 2006-2015 EsperTech Inc. All rights reserved.                        *
 * http://www.espertech.com/esper                                                          *
 * http://www.espertech.com                                                           *
 * ---------------------------------------------------------------------------------- *
 * The software in this package is published under the terms of the GPL license       *
 * a copy of which has been included with this distribution in the license.txt file.  *
 **************************************************************************************/
package com.espertech.esper.event.xml;

import com.espertech.esper.epl.generated.EsperEPL2GrammarParser;
import com.espertech.esper.epl.parse.ASTUtil;
import com.espertech.esper.type.IntValue;
import com.espertech.esper.util.ExecutionPathDebugLog;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;

/**
 * Parses event property names and transforms to XPath expressions. Supports
 * nested, indexed and mapped event properties.
 */
public class SimpleXMLPropertyParser
{
    /**
     * Return the xPath corresponding to the given property.
     * The propertyName String may be simple, nested, indexed or mapped.
     * @param ast is the property tree AST
     * @param propertyName is the property name to parse
     * @param rootElementName is the name of the root element for generating the XPath expression
     * @param defaultNamespacePrefix is the prefix of the default namespace
     * @param isResolvePropertiesAbsolute is true to indicate to resolve XPath properties as absolute props
     * or relative props
     * @return xpath expression
     */
    public static String walk(EsperEPL2GrammarParser.StartEventPropertyRuleContext ast, String propertyName, String rootElementName, String defaultNamespacePrefix, boolean isResolvePropertiesAbsolute)
    {
        StringBuilder xPathBuf = new StringBuilder();
        xPathBuf.append('/');
        if (isResolvePropertiesAbsolute)
        {
            if (defaultNamespacePrefix != null)
            {
                xPathBuf.append(defaultNamespacePrefix);
                xPathBuf.append(':');
            }
            xPathBuf.append(rootElementName);
        }

        List<EsperEPL2GrammarParser.EventPropertyAtomicContext> ctxs = ast.eventProperty().eventPropertyAtomic();
        if (ctxs.size() == 1) {
            xPathBuf.append(makeProperty(ctxs.get(0), defaultNamespacePrefix));
        }
        else {
            for (EsperEPL2GrammarParser.EventPropertyAtomicContext ctx : ctxs) {
                xPathBuf.append(makeProperty(ctx, defaultNamespacePrefix));
            }
        }

        String xPath = xPathBuf.toString();
        if ((ExecutionPathDebugLog.isDebugEnabled) && (log.isDebugEnabled())) {
            log.debug(".parse For property '" + propertyName + "' the xpath is '" + xPath + '\'');
        }

        return xPath;
    }

    private static String makeProperty(EsperEPL2GrammarParser.EventPropertyAtomicContext ctx, String defaultNamespacePrefix)
    {
        String prefix = "";
        if (defaultNamespacePrefix != null) {
            prefix = defaultNamespacePrefix + ":";
        }

        String unescapedIdent = ASTUtil.unescapeDot(ctx.eventPropertyIdent().getText());
        if (ctx.lb != null) {
            int index = IntValue.parseString(ctx.number().getText());
            int xPathPosition = index + 1;
            return '/' + prefix + unescapedIdent + "[position() = " + xPathPosition + ']';
        }

        if (ctx.lp != null) {
            String key = com.espertech.esper.type.StringValue.parseString(ctx.s.getText());
            return '/' + prefix + unescapedIdent + "[@id='" + key + "']";
        }

        return '/' + prefix + unescapedIdent;
    }

    private static final Log log = LogFactory.getLog(SimpleXMLPropertyParser.class);
}
