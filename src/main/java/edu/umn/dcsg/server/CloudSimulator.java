package edu.umn.dcsg.server;

import edu.umn.dcsg.common.Config;
import edu.umn.dcsg.common.GenericPayload;
import edu.umn.dcsg.common.ModelInfo;
import edu.umn.dcsg.common.SimulationHandler;
import edu.umn.dcsg.common.StreamTuple;

import java.util.List;
import java.util.Map;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

public class CloudSimulator extends AbstractCloud implements SimulationHandler
{
    SimulationResults   mResults;

    public CloudSimulator()
    {
        super();
        mResults = new SimulationResults();
    }

    public SimulationResults getResults()
    {
        return mResults;
    }

    public void handleEvent(final GenericPayload payload)
    {
        pHandleTuple(payload);
    }

    protected void pAnalyzeSamples(List<StreamTuple> trueAggregates)
    {
        int samplesGenerated = 0;
        int numModels = 0;

        final int winDuration = Config.getTrace().mWindowDuration;

        for(Map.Entry<Integer, ModelInfo> entry : mModelsMap.entrySet())
        {
            final int sCnt = pGenerateSamplesForModel(entry.getKey(), entry.getValue());
            samplesGenerated += sCnt;
            if(sCnt > 0) numModels++;
        }

        mResults.recordSampleCounts((int)mAllStats.getN(), samplesGenerated, numModels);

        Double totalSamples = 0.0;

        List<Integer> streamIds = pGetSortedKeys();
        final int streamCount = streamIds.size();

        if(streamCount == 0) return;

        double[] groupMeans = new double[streamCount];
        double[] vars = new double[streamCount];
        double[] mins = new double[streamCount];
        double[] maxes = new double[streamCount];
        double[] sampleCount = new double[streamCount];
        
        // Initialize arrays
        for(int i=0; i < streamCount; i++)
        {
            groupMeans[i] = sampleCount[i] = 0.0;
            vars[i] = 0.0;
            mins[i] = Double.MAX_VALUE;
            maxes[i] = Double.MIN_VALUE;
        }

        int streamIdx = 0;
        for(Integer streamId : streamIds)
        {
            SummaryStatistics sstats = new SummaryStatistics();

            for(StreamTuple st : mAllSamplesMap.get(streamId))
            {
                Double value = st.mValue;
                sstats.addValue(value);
                mins[streamIdx] = Math.min(mins[streamIdx], value);
                maxes[streamIdx] = Math.max(maxes[streamIdx], value);
                groupMeans[streamIdx] += value;
                sampleCount[streamIdx] += 1;
                totalSamples += 1;
            }

            for(StreamTuple st : mSimSamplesMap.get(streamId))
            {
                Double value = st.mValue;
                sstats.addValue(value);
                mins[streamIdx] = Math.min(mins[streamIdx], value);
                maxes[streamIdx] = Math.max(maxes[streamIdx], value);
                groupMeans[streamIdx] += value;
                sampleCount[streamIdx] += 1;
                totalSamples += 1;
            }

            groupMeans[streamIdx] /= sampleCount[streamIdx];
            vars[streamIdx] = sstats.getVariance();

            if(totalSamples == 1 || Double.isNaN(vars[streamIdx]))
                vars[streamIdx] = 0.0;

            streamIdx++;
        }

        for(int i = 0; i < streamCount; i++)
        {
            mResults.recordAvgWindowLoss(i, trueAggregates.get(4*i + 0).mValue, groupMeans[i]);
            mResults.recordVarWindowLoss(i, trueAggregates.get(4*i + 1).mValue, vars[i]);
            mResults.recordMinWindowLoss(i, trueAggregates.get(4*i + 2).mValue, mins[i]);
            mResults.recordMaxWindowLoss(i, trueAggregates.get(4*i + 3).mValue, maxes[i]);
        }
    }
}