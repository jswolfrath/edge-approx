package edu.umn.dcsg.client;

import edu.umn.dcsg.common.Config;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.stat.correlation.SpearmansCorrelation;
import org.apache.commons.math3.stat.correlation.KendallsCorrelation;

public class DependenceEstimator
{
    public static double getLinearDependence(final double[] x,
                                             final double[] y,
                                             final long[] xTimes,
                                             final long[] yTimes)
    {
        final int length = (int) Math.max(x.length, y.length);
        if(length == 1)
        {
            return 0.0;
        }

        double[] newx = interpolate(x, xTimes, length);
        double[] newy = interpolate(y, yTimes, length);

        double corr = Math.abs((new PearsonsCorrelation()).correlation(newx, newy));
        if(Double.isNaN(corr)) {
            return 0.0; // Probably a variance of zero in X or Y
        }
        return corr;
    }

    public static double getMonotonicDependence(final double[] x,
                                                final double[] y,
                                                final long[] xTimes,
                                                final long[] yTimes)
    {
        final int length = (int) Math.max(x.length, y.length);
        if(length == 1)
        {
            return 0.0;
        }

        double[] newx = interpolate(x, xTimes, length);
        double[] newy = interpolate(y, yTimes, length);

        double corr = Math.abs((new SpearmansCorrelation()).correlation(newx, newy));
        if(Double.isNaN(corr)) {
            return 0.0; // Probably a variance of zero in X or Y
        }
        return corr;
    }

    public static double[] interpolate(double[] x, long[] times, final int length)
    {
        assert(x.length > 0);
        assert(x.length == times.length);

        final int winDuration = Config.getTrace().mWindowDuration;

        if(x.length == length) return x;

        double[] newx = new double[length];

        for(int i=0; i < length; i++) newx[i] = 0.0;

        int lowestIdx = length;
        int highestIdx = -1;

        //
        // Plug in data points we have
        for(int i=0; i < x.length; i++)
        {
            int pointIdx = (int) Math.floor( ((double)length) * ((double)times[i]) / ((double)winDuration) );

            if(pointIdx >= newx.length){
                continue;
            }

            if(newx[pointIdx] == 0.0)
            {
                newx[pointIdx] = x[i];
            }
            else if(pointIdx + 1 < length)
            {
                pointIdx += 1;
                newx[pointIdx] = x[i];
            }

            if(pointIdx < lowestIdx) lowestIdx = pointIdx;
            if(pointIdx > highestIdx) highestIdx = pointIdx;
        }

        //
        // Fill in the ends
        for(int i=0; i < lowestIdx && lowestIdx < newx.length; i++)
            newx[i] = newx[lowestIdx];

        for(int i=length-1; i > highestIdx && highestIdx >= 0; i--)
            newx[i] = newx[highestIdx];

        //
        // Interpolate remaining points
        for(int i=lowestIdx; i < highestIdx; i++)
        {
            if(newx[i] == 0.0)
            {
                // need to interpolate this point
                // Yes, I know this is slow.
                int lb = i-1;
                if(lb < 0) lb = 0;
                int ub = i + 1;
                if(ub >= length) ub = length-1;
                else while(newx[ub] == 0.0 && ub < length - 1) ub++;

                double dist = ((double)(i-lb)) / ((double)(ub - lb));
                newx[i] = newx[lb] + dist*(newx[ub] - newx[lb]);
            }
        }
    
        return newx;
    }
}
