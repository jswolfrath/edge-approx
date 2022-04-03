package edu.umn.dcsg.server;

import java.lang.Exception;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.commons.math3.random.EmpiricalDistribution;

public class SimulationResults
{
    protected Integer                       mTotalRealSamples;
    protected Integer                       mTotalSimSamples;
    protected Integer                       mTotalModels;

    protected Map<Integer, List<Double>>    mStreamActual;
    protected Map<Integer, List<Double>>    mStreamEstimated;

    protected Map<Integer, List<Double>>    mStreamActualVar;
    protected Map<Integer, List<Double>>    mStreamEstimatedVar;

    protected Map<Integer, List<Double>>    mStreamActualMin;
    protected Map<Integer, List<Double>>    mStreamEstimatedMin;

    protected Map<Integer, List<Double>>    mStreamActualMax;
    protected Map<Integer, List<Double>>    mStreamEstimatedMax;

    public SimulationResults()
    {
        mStreamActual = new HashMap<Integer, List<Double>>();
        mStreamEstimated = new HashMap<Integer, List<Double>>();

        mStreamActualVar = new HashMap<Integer, List<Double>>();
        mStreamEstimatedVar = new HashMap<Integer, List<Double>>();

        mStreamActualMin = new HashMap<Integer, List<Double>>();
        mStreamEstimatedMin = new HashMap<Integer, List<Double>>();

        mStreamActualMax = new HashMap<Integer, List<Double>>();
        mStreamEstimatedMax = new HashMap<Integer, List<Double>>();

        mTotalRealSamples = 0;
        mTotalSimSamples = 0;
        mTotalModels = 0;
    }

    public void clear()
    {
        mStreamActual.clear();
        mStreamEstimated.clear();

        mStreamActualVar.clear();
        mStreamEstimatedVar.clear();

        mStreamActualMin.clear();
        mStreamEstimatedMin.clear();

        mStreamActualMax.clear();
        mStreamEstimatedMax.clear();

        mTotalRealSamples = 0;
        mTotalSimSamples = 0;
        mTotalModels = 0;
    }

    public void print()
    {
        System.out.println("SimResults:");
        System.out.println("  Real Samples:  " + mTotalRealSamples);
        System.out.println("  Sim Samples:   " + mTotalSimSamples);
        System.out.println("  Cost Incurred: " + getTotalCost());
    }

    public int getTotalCost()
    {
        return mTotalRealSamples + mTotalModels;
    }

    public double getAVGNormalizedRMSEAcrossAllStreams()
    {
        double err = 0.0;
        double numStreams = 0.0;

        for(Integer streamId : mStreamActual.keySet())
        {
            final List<Double> actual = pGetStreamActualList(streamId);
            final List<Double> est = pGetStreamEstimatedList(streamId);
            double sErr = pGetNormRMSE(actual, est);
            err += sErr;
            numStreams += 1.0;
        }

        return err / numStreams;
    }

    public double getVARNormalizedRMSEAcrossAllStreams()
    {
        double err = 0.0;
        double numStreams = 0.0;

        for(Integer streamId : mStreamActual.keySet())
        {
            final List<Double> actual = pGetStreamActualVarList(streamId);
            final List<Double> est = pGetStreamEstimatedVarList(streamId);
            double sErr = pGetNormRMSE(actual, est);
            err += sErr;
            numStreams += 1.0;
        }

        return err / numStreams;
    }

    public double getMINNormalizedRMSEAcrossAllStreams()
    {
        double err = 0.0;
        double numStreams = 0.0;

        for(Integer streamId : mStreamActual.keySet())
        {
            final List<Double> actual = pGetStreamActualMinList(streamId);
            final List<Double> est = pGetStreamEstimatedMinList(streamId);
            
            double sErr = pGetNormRMSE(actual, est);
            err += sErr;
            numStreams += 1.0;
        }

        return err / numStreams;
    }

    public double getMAXNormalizedRMSEAcrossAllStreams()
    {
        double err = 0.0;
        double numStreams = 0.0;

        for(Integer streamId : mStreamActual.keySet())
        {
            final List<Double> actual = pGetStreamActualMaxList(streamId);
            final List<Double> est = pGetStreamEstimatedMaxList(streamId);
            double sErr = pGetNormRMSE(actual, est);
            err += sErr;
            numStreams += 1.0;
        }

        return err / numStreams;
    }

    public void recordAvgWindowLoss(final Integer streamId,
                                    final double actualMean,
                                    final double estimatedMean)
    {
        pGetStreamActualList(streamId).add(actualMean);
        pGetStreamEstimatedList(streamId).add(estimatedMean);
    }

    public void recordVarWindowLoss(final Integer streamId,
                                    final double actualStd,
                                    final double estimatedStd)
    {
        pGetStreamActualVarList(streamId).add(actualStd);
        pGetStreamEstimatedVarList(streamId).add(estimatedStd);
    }

    public void recordMinWindowLoss(final Integer streamId,
                                    final double actualMax,
                                    final double estimatedMax)
    {
        pGetStreamActualMinList(streamId).add(actualMax);
        pGetStreamEstimatedMinList(streamId).add(estimatedMax);
    }

    public void recordMaxWindowLoss(final Integer streamId,
                                    final double actualMax,
                                    final double estimatedMax)
    {
        pGetStreamActualMaxList(streamId).add(actualMax);
        pGetStreamEstimatedMaxList(streamId).add(estimatedMax);
    }

    public void recordSampleCounts(final Integer real, final Integer sim,
                                   final Integer numModels)
    {
        mTotalRealSamples += real;
        mTotalSimSamples += sim;
        mTotalModels += numModels;
    }

    protected List<Double> pGetStreamActualList(final Integer streamId)
    {
        List<Double> actualList = mStreamActual.get(streamId);
        if(actualList == null)
        {
            actualList = new ArrayList<Double>();
            mStreamActual.put(streamId, actualList);
        }

        return actualList;
    }

    protected List<Double> pGetStreamEstimatedList(final Integer streamId)
    {
        List<Double> estList = mStreamEstimated.get(streamId);
        if(estList == null)
        {
            estList = new ArrayList<Double>();
            mStreamEstimated.put(streamId, estList);
        }

        return estList;
    }

    protected List<Double> pGetStreamActualVarList(final Integer streamId)
    {
        List<Double> actualList = mStreamActualVar.get(streamId);
        if(actualList == null)
        {
            actualList = new ArrayList<Double>();
            mStreamActualVar.put(streamId, actualList);
        }

        return actualList;
    }

    protected List<Double> pGetStreamEstimatedVarList(final Integer streamId)
    {
        List<Double> estList = mStreamEstimatedVar.get(streamId);
        if(estList == null)
        {
            estList = new ArrayList<Double>();
            mStreamEstimatedVar.put(streamId, estList);
        }

        return estList;
    }

    protected List<Double> pGetStreamActualMinList(final Integer streamId)
    {
        List<Double> actualList = mStreamActualMin.get(streamId);
        if(actualList == null)
        {
            actualList = new ArrayList<Double>();
            mStreamActualMin.put(streamId, actualList);
        }

        return actualList;
    }

    protected List<Double> pGetStreamEstimatedMinList(final Integer streamId)
    {
        List<Double> estList = mStreamEstimatedMin.get(streamId);
        if(estList == null)
        {
            estList = new ArrayList<Double>();
            mStreamEstimatedMin.put(streamId, estList);
        }

        return estList;
    }

    protected List<Double> pGetStreamActualMaxList(final Integer streamId)
    {
        List<Double> actualList = mStreamActualMax.get(streamId);
        if(actualList == null)
        {
            actualList = new ArrayList<Double>();
            mStreamActualMax.put(streamId, actualList);
        }

        return actualList;
    }

    protected List<Double> pGetStreamEstimatedMaxList(final Integer streamId)
    {
        List<Double> estList = mStreamEstimatedMax.get(streamId);
        if(estList == null)
        {
            estList = new ArrayList<Double>();
            mStreamEstimatedMax.put(streamId, estList);
        }

        return estList;
    }

    protected double[] pListToDoubleArray(final List<Double> listParm)
    {
        double[] arr = new double[listParm.size()];
        for(int i=0; i < listParm.size(); i++)
        {
            arr[i] = listParm.get(i);
        }
        return arr;
    }

    protected double pGetNormRMSE(final List<Double> actualList,
                                  final List<Double> estimatedList)
    {
        assert(actualList.size() == estimatedList.size());

        final int len = actualList.size();
        double sum = 0.0;
        double trueMean = 0.0;

        for(int i=0; i < len; i++)
        {
            final double actual = actualList.get(i);
            final double est = estimatedList.get(i);
            sum += Math.pow(actual - est, 2.0);
            trueMean += actual;
        }

        trueMean /= len;
        return Math.sqrt(sum / (double)len) / (1.0 + Math.abs(trueMean));
    }
}
