package edu.umn.dcsg.common;

import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;

public class RegModel extends OLSMultipleLinearRegression
{
    public double getSanatizedRSquared()
    {
        double r2 = calculateRSquared();
        final double yvar = calculateYVariance();
        if(Double.isNaN(yvar) || yvar == 0.0) {
            return 1.0;
        }

        return r2;
    }
}