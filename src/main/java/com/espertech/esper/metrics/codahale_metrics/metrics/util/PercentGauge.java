/***************************************************************************************
 * Attribution Notice
 *
 * This file is imported from Metrics (https://github.com/codahale/metrics subproject metrics-core).
 * Metrics is Copyright (c) 2010-2012 Coda Hale, Yammer.com
 * Metrics is Published under Apache Software License 2.0, see LICENSE in root folder.
 *
 * Thank you for the Metrics developers efforts in making their library available under an Apache license.
 * EsperTech incorporates Metrics version 0.2.2 in source code form since Metrics depends on SLF4J
 * and this dependency is not possible to introduce for Esper.
 * *************************************************************************************
 */
package com.espertech.esper.metrics.codahale_metrics.metrics.util;

/**
 * A {@link RatioGauge} extension which returns a percentage, not a ratio.
 */
public abstract class PercentGauge extends RatioGauge {
    private static final int ONE_HUNDRED = 100;

    @Override
    public Double value() {
        return super.value() * ONE_HUNDRED;
    }
}
