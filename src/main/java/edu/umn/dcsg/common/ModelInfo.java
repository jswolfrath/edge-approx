package edu.umn.dcsg.common;

//import clojure.core.protocols__init;

import org.apache.commons.math3.linear.RealVector;

// Information related to an imputation module that needs to be
// sent over the network.
public class ModelInfo
{
    protected final String DELIMITER = ":";

    protected Integer   mPredictorId;
    protected Integer   mSampleCount;
    protected Double    mMeanY;
    protected Double    mStdErr;
    protected double[]  mBeta;

    public ModelInfo(int predictor, int sampleCount, double meanY, RegModel model)
    {
        mPredictorId = predictor;
        mSampleCount = sampleCount;
        mMeanY = meanY;
        mBeta = model.estimateRegressionParameters();
        mStdErr = model.estimateRegressionStandardError();
    }

    public ModelInfo(String serialized)
    {
        String[] tokens = serialized.split(DELIMITER);

        mPredictorId = Integer.parseInt(tokens[0]);
        mSampleCount = Integer.parseInt(tokens[1]);
        mMeanY = Double.parseDouble(tokens[2]);
        mStdErr = Double.parseDouble(tokens[3]);

        final int len = tokens.length - 4;
        mBeta = new double[len];

        for(int i = 0; i < len; i++)
        {
            mBeta[i] = Double.parseDouble(tokens[4+i]);
        }
    }

    public Double evaluate(double x)
    {
        if(Config.USE_MEAN_IMPUTATION)
        {
            return mMeanY;
        }

        double result = 0.0;
        double[] xpoint = null;
        if(mBeta.length == 2)
        {
            xpoint = new double[] { 1.0, x };
        }
        else
        {
            xpoint = new double[] { 1.0, x,
                                    Math.pow(x, 2.0),
                                    Math.pow(x, 3.0)};
        }
    
        for(int i=0; i < mBeta.length; i++)
        {
            result += mBeta[i] * xpoint[i]; 
        }

        return result;
    }

    public int getSampleCount()
    {
        return mSampleCount;
    }

    public int getPredictorId()
    {
        return mPredictorId;
    }

    public double getStdErr()
    {
        return mStdErr;
    }

    public double getDummyPrediction()
    {
        return mMeanY + mStdErr;
    }

    public String toString()
    {
        String result = mPredictorId.toString() +
                        DELIMITER +
                        mSampleCount.toString() +
                        DELIMITER +
                        mMeanY.toString() +
                        DELIMITER +
                        mStdErr.toString();

        for(int i = 0; i < mBeta.length; i++)
        {
            Double d = mBeta[i];
            result += (DELIMITER + d.toString());
        }
    
        return result;
    }
}
