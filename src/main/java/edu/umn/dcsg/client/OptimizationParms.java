package edu.umn.dcsg.client;

import java.util.Arrays;

public class OptimizationParms
{
    public final Integer    mNumStreams;
    public final Double     mSamplesAllowed;
    public long[]           mStrataSizes;
    public int[]            mPredictorStream;
    public double[]         mPredictorDependence;
    public double[]         mStrataCosts;
    public double[]         mStrataMeans;
    public double[]         mStrataVariance;
    public double[]         mVariationBounds;
    public double[]         mExpectedConditionalVariances;

    protected static final String   DELIMITER = "\n";

    public OptimizationParms(double   samplesAllowed,
                             long[]   strataSizes,
                             int[]    predictorStream,
                             double[] predictorDependence,
                             double[] strataCosts,
                             double[] strataMeans,
                             double[] strataVariance,
                             double[] variationBounds,
                             double[] expectedConditionalVariances)
    {
        mNumStreams = strataSizes.length;

        mSamplesAllowed = samplesAllowed;
        mStrataSizes = strataSizes;
        mPredictorStream = predictorStream;
        mPredictorDependence = predictorDependence;
        mStrataCosts = strataCosts;
        mStrataMeans = strataMeans;
        mStrataVariance = strataVariance;
        mVariationBounds = variationBounds;
        mExpectedConditionalVariances = expectedConditionalVariances;

        for(int i=0; i < mNumStreams; i++)
        {
            if(mStrataVariance[i] == 0.0)
            {
                mPredictorDependence[i] = 0.0;
                mVariationBounds[i] = 1.0;
                mExpectedConditionalVariances[i] = 1.0;

                // Make sure we always have positive variance
                mStrataVariance[i] += 0.001;
            }
        }
    }

    public String toString()
    {
        String result = "";

        result += mNumStreams.toString();
        result += DELIMITER;
    
        result += mSamplesAllowed.toString();
        result += DELIMITER;

        result += Arrays.toString(mStrataSizes);
        result += DELIMITER;

        result += Arrays.toString(mPredictorStream);
        result += DELIMITER;

        result += Arrays.toString(mPredictorDependence);
        result += DELIMITER;

        result += Arrays.toString(mStrataCosts);
        result += DELIMITER;

        result += Arrays.toString(mStrataMeans);
        result += DELIMITER;

        result += Arrays.toString(mStrataVariance);
        result += DELIMITER;

        result += Arrays.toString(mVariationBounds);
        result += DELIMITER;

        result += Arrays.toString(mExpectedConditionalVariances);
        result += DELIMITER;
        
        return result;
    }
}
