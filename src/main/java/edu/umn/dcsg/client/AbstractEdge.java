package edu.umn.dcsg.client;

import edu.umn.dcsg.common.Config;
import edu.umn.dcsg.common.GenericPayload;
import edu.umn.dcsg.common.ModelInfo;
import edu.umn.dcsg.common.RegModel;
import edu.umn.dcsg.common.StreamTuple;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

import java.io.BufferedReader;
import java.io.InputStreamReader;

public abstract class AbstractEdge
{
    protected Object             mLock;
    protected TTLDataCache       mDataCache;
    protected int                mWindowId;
    protected int                mTotalObservedPoints;
    protected double             mAllowedCost;

    public AbstractEdge()
    {
        mLock = new Object();
        mDataCache = new TTLDataCache();
        mWindowId = 1;
        mTotalObservedPoints = 0;
        mAllowedCost = 0;
    }

    // Each implementation will need to determine how to forward data
    protected abstract void pSendPayload(final GenericPayload gp);

    // Each implementation can implement its own handler
    protected abstract void pHandleTuple(final GenericPayload gp);

    public double getTotalPoints()
    {
        return mTotalObservedPoints;
    }

    public double getAllowedCost()
    {
        return mAllowedCost;
    }

    public void flush()
    {
        synchronized (mLock)
        {
            pForwardSamplesLocked();
            mDataCache.clear();
            mWindowId++;
        }
    }

    protected void pInsertValue(final GenericPayload payload)
    {
        String streamId = payload.getStreamId();
        double value = Double.parseDouble(payload.getValue());
        long time = Long.parseLong(payload.getTime());
        String metadata = payload.getMetadata();

        synchronized(mLock)
        {
            mDataCache.insert(Integer.parseInt(streamId), value, time, metadata);
        }
    }

    protected void pWriteWindowHeader()
    {
        GenericPayload header = new GenericPayload(GenericPayload.TYPE_HEADER);
        pSendPayload(header);
    }

    protected void pWriteWindowTrailer()
    {
        GenericPayload trailer = new GenericPayload(GenericPayload.TYPE_TRAILER);
        trailer.setValue(mDataCache.exactAggregatesToString());
        pSendPayload(trailer);
    }

    protected void pSendModel(Integer streamId, ModelInfo mInfo)
    {
        GenericPayload payload = new GenericPayload(GenericPayload.TYPE_IMPUTATION_MODEL);

        payload.setStreamId(streamId.toString());
        payload.setValue(mInfo.toString());
        pSendPayload(payload);
    }

    protected void pSendSamples(List<StreamTuple> samples)
    {
        for(StreamTuple pair : samples)
        {
            GenericPayload payload = new GenericPayload(GenericPayload.TYPE_TUPLE);
            payload.setStreamId(pair.mStreamId.toString());
            payload.setValue(pair.mValue.toString());
            payload.setTime(pair.mTime.toString());
            pSendPayload(payload);
        }
    }

    protected void pForwardSamplesLocked()
    {
        pWriteWindowHeader();

        final int streamCount = mDataCache.getNumStreams();
        final int samplesObserved = mDataCache.getTotalObservedValues().intValue();
        mTotalObservedPoints += samplesObserved;

        double computedSampleCount = (((double)samplesObserved) * Config.SAMPLING_FRACTION);
        final double samplesAllowed = Math.max(streamCount, computedSampleCount);
        mAllowedCost += samplesAllowed;

        LinearCostFunction[] costs = new LinearCostFunction[streamCount];
        List<Integer> streamIds = mDataCache.getStreamIds();
        double[] costReal = new double[streamCount];

        final double mid = (double)streamCount / 2.0;
        for(int i=0; i < streamCount; i++)
        {
            costReal[i] = 1.0;
            costs[i] = LinearCostFunction.UNIT_COST;
        }

        if(Config.STRATEGY == Config.SRS_STRATEGY)
        {
            List<StreamTuple> samples = mDataCache.getSimpleRandomSample((int)samplesAllowed);
            pSendSamples(samples);
        }
        else if(Config.STRATEGY == Config.STRATIFIED_EVEN_STRATEGY)
        {
            int[] sampleSizes2 = SampleSizeCalculator.getProportionalAllocation(samplesAllowed,
                                                                                mDataCache.getObservedCountByStreamDouble());
            for(int i=0; i < streamCount; i++)
            {
                List<StreamTuple> samples = mDataCache.getSampleForStream(streamIds.get(i),
                                                                        (int)Math.round(sampleSizes2[i]));
                if(samples != null)
                {
                    pSendSamples(samples);
                }
            }
        }
        else if(Config.STRATEGY == Config.NEYMAN_STRATEGY)
        {
            int[] sampleSizes2 = SampleSizeCalculator.getOptimalAllocation((double)samplesAllowed,
                                                                           mDataCache.getObservedCountByStreamDouble(),
                                                                           mDataCache.getStreamVariances(),
                                                                           costs);
            for(int i=0; i < streamCount; i++)
            {
                List<StreamTuple> samples = mDataCache.getSampleForStream(streamIds.get(i),
                                                                        (int)Math.round(sampleSizes2[i]));
                if(samples != null)
                {
                    pSendSamples(samples);
                }
            }
        }
        else if(Config.STRATEGY == Config.IMPUTATION_STRATEGY)
        {
            double[] means = mDataCache.getStreamMeans();
            double[] variance = mDataCache.getStreamVariances();
            double[] vBounds = mDataCache.generateVBounds();
            int[] predictors = mDataCache.getPredictors();
            double[] sCosts = new double[variance.length];
            long[] popSizes = mDataCache.getObservedCountByStream();
            double[] sECV = mDataCache.getExpectedConditionalVariance(predictors);

            for(int i=0; i < variance.length; i++)
            {
                sCosts[i] = costs[i].getCostPerSample();
            }

            OptimizationParms optParms = new OptimizationParms((double)samplesAllowed,
                                          popSizes, predictors,
                                          mDataCache.getDependenceForPredictors(),
                                          sCosts, means, variance, vBounds, sECV);

            OptimizationSolver solver = new OptimizationSolver(optParms);
            double[] ssizes = null;

            try {
                ssizes = solver.compute((double)samplesAllowed);
            } catch(Exception e) {
                System.err.println("Encountered unexpected exception during optimization.");
                System.err.println(e);
                System.exit(1);
            }

            int streamIdx = 0;
            for(Integer streamId : streamIds)
            {
                final double ssize = ssizes[streamCount + streamIdx];
                final int numPredictions = (int)Math.round(ssizes[streamCount + streamIdx]);
                final int predictor = predictors[streamIdx];

                if(numPredictions > 0)
                {
                    RegModel reg = mDataCache.getPredictiveModel(streamIds.get(streamIdx),
                                                                 streamIds.get(predictor));
                    if(reg != null)
                    {
                        ModelInfo model = new ModelInfo(streamIds.get(predictor),
                                                        numPredictions, means[streamIdx],
                                                        reg);

                        pSendModel(streamIds.get(streamIdx), model);
                    }
                    else {
                        // Something went wrong... Send 1 real sample instead.
                        ssizes[streamIdx] += 1.0;
                    }
                }

                streamIdx++;
            }

            int edgeSamplesSent = 0;
            for(int i=0; i < streamCount; i++)
            {
                int ss = (int) Math.round(ssizes[i]);
                List<StreamTuple> samples = mDataCache.getSampleForStream(streamIds.get(i), ss);
                edgeSamplesSent += samples.size();
                pSendSamples(samples);
            }
        }

        pWriteWindowTrailer();
    }
}