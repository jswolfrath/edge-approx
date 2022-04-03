package edu.umn.dcsg.server;

import edu.umn.dcsg.client.TTLDataCache;

import edu.umn.dcsg.common.Config;
import edu.umn.dcsg.common.GenericPayload;
import edu.umn.dcsg.common.ModelInfo;
import edu.umn.dcsg.common.RegModel;
import edu.umn.dcsg.common.StreamTuple;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

public abstract class AbstractCloud
{
    protected Map<Integer, ModelInfo>       mModelsMap;
    protected Object                        mLock;

    protected Map<Integer, List<StreamTuple>> mAllSamplesMap;
    protected Map<Integer, List<StreamTuple>> mSimSamplesMap;
    protected SummaryStatistics             mAllStats;

    protected Integer                       mWindowId;

    public AbstractCloud()
    {
        mAllSamplesMap = new HashMap<Integer, List<StreamTuple>>();
        mSimSamplesMap = new HashMap<Integer, List<StreamTuple>>();
        mModelsMap = new HashMap<Integer, ModelInfo>();

        mAllStats = new SummaryStatistics();

        mLock = new Object();
        mWindowId = 1;
    }

    // Subclasses can optionally analyze local data
    protected abstract void pAnalyzeSamples(List<StreamTuple> trueAggregates);

    protected void pHandleTuple(final GenericPayload payload)
    {
        synchronized(mLock)
        {
            if(payload.isHeader())
            {
                // Nothing to do.
            }
            else if(payload.isTrailer())
            {
                List<StreamTuple> trueAggregates = TTLDataCache.stringToExactAggregates(payload.getValue());
                pAnalyzeSamples(trueAggregates);

                mModelsMap.clear();
                mAllSamplesMap.clear();
                mSimSamplesMap.clear();
                mAllStats.clear();
                mWindowId++;
            }
            else if(payload.isTuple())
            {
                int sourceStream = Integer.parseInt(payload.getStreamId());
                double value = Double.parseDouble(payload.getValue());
                long time = Long.parseLong(payload.getTime());
                String metadata = payload.getMetadata();

                StreamTuple sTuple = new StreamTuple(sourceStream, value, time, metadata);

                pInitializeStreamIfNeeded(sourceStream);

                mAllSamplesMap.get(sourceStream).add(sTuple);
                mAllStats.addValue(value);
            }
            else if(payload.isImputationModel())
            {
                int sourceStream = Integer.parseInt(payload.getStreamId());
                mModelsMap.put(sourceStream, new ModelInfo(payload.getValue()));
            }
        }
    }

    protected List<Integer> pGetSortedKeys()
    {
        List<Integer> sortedKeys = new ArrayList<Integer>(mAllSamplesMap.keySet());
        Collections.sort(sortedKeys);
        return sortedKeys;
    }

    protected void pInitializeStreamIfNeeded(int streamId)
    {
        if(mAllSamplesMap.get(streamId) == null)
        {
            mAllSamplesMap.put(streamId, new ArrayList<StreamTuple>());
            mSimSamplesMap.put(streamId, new ArrayList<StreamTuple>());
        }
    }

    protected boolean pValidateDouble(double x)
    {
        return (!Double.isNaN(x)) && (!Double.isInfinite(x));
    }

    // @returns the number of samples generated from the model
    protected int pGenerateSamplesForModel(int targetStream, ModelInfo model)
    {
        if(model == null) return 0;
        if(model.getSampleCount() < 1) return 0;

        int samplesGenerated = 0;
        final double modelStdErr = model.getStdErr();

        pInitializeStreamIfNeeded(targetStream);

        List<StreamTuple> xVals = mAllSamplesMap.get(model.getPredictorId());
        List<StreamTuple> yVals = mAllSamplesMap.get(targetStream);

        if(xVals == null)
        {
            double dummy = model.getDummyPrediction();
            StreamTuple yPoint = new StreamTuple(targetStream, dummy, (long)0);
            mSimSamplesMap.get(targetStream).add(yPoint);
            System.err.println("WARNING: Constraints did not work. Skipping model... " + dummy);
            return 1;
        }

        final List<StreamTuple> xSim = pGetOptimalXVals(xVals, yVals, model.getSampleCount());
        for(int i=0; i < xSim.size(); i++)
        {
            final StreamTuple xPoint = xSim.get(i);
            final double x = xPoint.mValue;
            double expectedValue = model.evaluate(x);

            StreamTuple yPoint = new StreamTuple(targetStream, expectedValue, xPoint.mTime);
            mSimSamplesMap.get(targetStream).add(yPoint);
            samplesGenerated++;
        }

        return samplesGenerated;
    }

    protected List<StreamTuple> pGetOptimalXVals(List<StreamTuple> x, List<StreamTuple> y, int count)
    {
        List<StreamTuple> samples = new ArrayList<StreamTuple>();

        if(Config.USE_THINNING)
        {
            if(y.size() == 0)
            {
                int inc = (int) Math.floor(Math.max(1, x.size() / count));
                for(int i=0; i < x.size(); i += inc)
                {
                    samples.add(x.get(i));
                }
            }
            else if(x.size() == 0)
            {
                System.err.println("SEVERE: We don't have x values to simulate from");
            }
            else
            {
                List<StreamTuple> newx = new ArrayList<>(x);
                for(int i=0; i < count; i++)
                {
                    long bestDist = -1;
                    int bestIdx = -1;

                    for(int j=0; j < newx.size(); j++)
                    {
                        long candDist = 1000000000;
                        StreamTuple candidate = newx.get(j);

                        for(int k=0; k < y.size(); k++)
                        {
                            long dist = (long)Math.abs(y.get(k).mTime - candidate.mTime);
                            if(dist < candDist) candDist = dist;
                        }

                        if(bestDist < candDist)
                        {
                            bestIdx = j;
                            bestDist = candDist;
                        }
                    }

                    if(bestIdx == -1)
                    {
                        break;
                    }

                    samples.add(newx.get(bestIdx));
                    newx.remove(bestIdx);
                }
            }
        }
        else
        {
            Collections.shuffle(x);

            for(int i=0; i < count && i < x.size(); i++)
            {
                samples.add(x.get(i));
            }
        }

        return samples;
    }
}