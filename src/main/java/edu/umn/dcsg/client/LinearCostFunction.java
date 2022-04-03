package edu.umn.dcsg.client;

public class LinearCostFunction
{
    // Cost function for evaulation based solely on sample size
    public static LinearCostFunction UNIT_COST = new LinearCostFunction(0, 1);

    protected double    mBaseCost;
    protected double    mCostPerSample;

    public LinearCostFunction(double base, double costPerSample)
    {
        mBaseCost = base;
        mCostPerSample = costPerSample;
    }

    public double evaluate(double sampleCount)
    {
        return mBaseCost + sampleCount * mCostPerSample;
    }

    public double getBaseCost()
    {
        return mBaseCost;
    }

    public double getCostPerSample()
    {
        return mCostPerSample;
    }
}
