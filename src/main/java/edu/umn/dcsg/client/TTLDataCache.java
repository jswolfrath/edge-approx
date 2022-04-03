package edu.umn.dcsg.client;

import edu.umn.dcsg.common.Config;
import edu.umn.dcsg.common.StreamTuple;
import edu.umn.dcsg.common.RegModel;

import java.lang.IllegalArgumentException;
import java.lang.StringBuilder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Random;
import java.util.Comparator;

import org.apache.commons.math3.stat.descriptive.SummaryStatistics;
import org.apache.commons.math3.linear.SingularMatrixException;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;

public class TTLDataCache
{
    // Internal class for handling stream data
    protected class StreamData
    {
        public SummaryStatistics    mSummary;
        public List<Double>         mObservedValues;
        public List<Long>           mTimestamps;
        public List<String>         mMetadata;

        public StreamData()
        {
            mSummary = new SummaryStatistics();
            mObservedValues = new ArrayList<Double>();
            mTimestamps = new ArrayList<Long>();
            mMetadata = new ArrayList<String>();
        }

        public void addValue(Double value, Long time, String metadata)
        {
            mSummary.addValue(value);
            mObservedValues.add(value);
            mMetadata.add(metadata);
            mTimestamps.add(time);
        }

        public double[] getArray()
        {
            return mObservedValues.stream().mapToDouble(Double::doubleValue).toArray();
        }

        public long[] getTimeArray()
        {
            return mTimestamps.stream().mapToLong(Long::longValue).toArray();
        }
    }
    
    protected static final String DELIMITER = ":";

    protected SummaryStatistics         mOverallStats;
    protected Map<Integer, StreamData>  mStreams;
    protected Random                    mRNG;

    public TTLDataCache()
    {
        mOverallStats = new SummaryStatistics();
        mStreams = new HashMap<Integer, StreamData>();
        mRNG = new Random(0x12345678);
    }

    public void insert(Integer streamId, Double value, Long time, String metadata)
    {
        pInitializeStreamIfNull(streamId);
        mStreams.get(streamId).addValue(value, time, metadata);

        if(Double.isNaN(value) || Double.isInfinite(value))
        {
            throw new IllegalArgumentException("Caching invalid value.");
        }

        mOverallStats.addValue(value);
    }

    public void clear()
    {
        mOverallStats.clear();
        mStreams.clear();
    }

    public Long getTotalObservedValues()
    {
        return mOverallStats.getN();
    }

    public static List<StreamTuple> stringToExactAggregates(String payload)
    {
        List<StreamTuple> means = new ArrayList<StreamTuple>();
        String[] entries = payload.split(TTLDataCache.DELIMITER);
        
        for(int i=0; i < entries.length; i++)
        {
            means.add(new StreamTuple(i/4, Double.parseDouble(entries[i])));
        }

        return means;
    }

    public String exactAggregatesToString()
    {
        StringBuilder sBuilder = new StringBuilder();

        for(Map.Entry<Integer, StreamData> entry : mStreams.entrySet())
        {
            sBuilder.append(entry.getValue().mSummary.getMean());
            sBuilder.append(TTLDataCache.DELIMITER);
            sBuilder.append(entry.getValue().mSummary.getPopulationVariance());
            sBuilder.append(TTLDataCache.DELIMITER);
            sBuilder.append(entry.getValue().mSummary.getMin());
            sBuilder.append(TTLDataCache.DELIMITER);
            sBuilder.append(entry.getValue().mSummary.getMax());
            sBuilder.append(TTLDataCache.DELIMITER);
        }

        return sBuilder.toString();
    }

    public SummaryStatistics getOverallSummaryStats()
    {
        return mOverallStats;
    }

    public int getNumStreams()
    {
        return pGetSortedKeys().size();
    }

    public List<StreamTuple> getSimpleRandomSample(int numSamples)
    {
        List<StreamTuple> samples = new ArrayList<StreamTuple>();
        final int numStreams = getNumStreams();
        long[] observed = getObservedCountByStream();

        int usedSamples = 0;
        int[] allocation = new int[numStreams];

        // Guarantee at least one sample per stream
        for(int i=0; i < numStreams; i++)
        {
            allocation[i] = 1;
            usedSamples++;
        }
        
        final int maxRetries = 100;
        int retries = 0;
        while(usedSamples < numSamples)
        {
            final int randIdx = mRNG.nextInt(numStreams);

            if(allocation[randIdx] + 1 > observed[randIdx])
            {
                retries++;
                if(retries > maxRetries) break;

                continue;
            }

            allocation[randIdx]++;
            usedSamples++;
        }

        int streamIdx = 0;
        for(Map.Entry<Integer, StreamData> entry : mStreams.entrySet())
        {
            samples.addAll(getSampleForStream(entry.getKey(), allocation[streamIdx]));
            streamIdx++;
        }

        return samples;
    }

    public List<StreamTuple> getSampleForStream(int streamId, int numSamples)
    {
        List<StreamTuple> samples = null;
        StreamData sData = mStreams.get(streamId);

        if(numSamples == 0) return new ArrayList<StreamTuple>();

        final int len = sData.mObservedValues.size();
        final int inc = (len / numSamples);
        samples = new ArrayList<StreamTuple>();
        int extraJump = 0;

        if(numSamples > len) numSamples = len;

        if(numSamples == 1)
        {
            final int randIdx = mRNG.nextInt(len);
            Double val = sData.mObservedValues.get(randIdx);
            Long time = sData.mTimestamps.get(randIdx);
            samples.add(new StreamTuple(streamId, val, time));
            return samples;
        }

        if(Config.USE_THINNING && inc > 1)
        {
            // Try to select a random starting point
            int startIdx = 0;
            final int shiftsAvailable = len - (inc * (numSamples-1));
            if(shiftsAvailable > 0)
            {
                // We can optionally shift the starting point at least one
                startIdx += mRNG.nextInt(shiftsAvailable);
            }

            int samplesSent = 0;
            for(int i=startIdx; i < len && samplesSent < numSamples; i += inc)
            {
                Double val = sData.mObservedValues.get(i);
                Long time = sData.mTimestamps.get(i);
                samples.add(new StreamTuple(streamId, val, time));
                samplesSent++;
            }

            assert(samples.size() == numSamples);
        }
        else
        {
            Collections.shuffle(sData.mObservedValues);

            for(int i=0; i < numSamples; i++)
            {
                Double val = sData.mObservedValues.get(i);
                Long time = sData.mTimestamps.get(i);
                samples.add(new StreamTuple(streamId, val, time));
            }
        }

        return samples;
    }

    public int[] getPredictors()
    {
        final int streamCount = getNumStreams();
        double[][] dependence = getDependenceMatrix();
        final double[] varianceY = getStreamVariances();
        double[] bestMatches = new double[streamCount];
        int[] predictors = new int[streamCount];

        // Just use the best predictor for each stream
        for(int i=0; i < streamCount; i++)
        {
            predictors[i] = -1;
            bestMatches[i] = -1;

            for(int j=0; j < streamCount; j++)
            {
                if(i == j) continue;

                if(predictors[i] == -1)
                {
                    predictors[i] = j;
                    bestMatches[i] = dependence[i][j];
                }
                else if(dependence[i][j] > bestMatches[i])
                {
                    predictors[i] = j;
                    bestMatches[i] = dependence[i][j];
                }
            }
        }

        return predictors;
    }

    protected double[] getDependenceForPredictors()
    {
        double[][] dependence = getDependenceMatrix();
        final int streamCount = dependence[0].length;
        double[] predictorDependence = new double[streamCount];

        for(int i=0; i < streamCount; i++)
        {
            predictorDependence[i] = 0.0;

            for(int j=0; j < streamCount; j++)
            {
                if(i == j) continue;

                if(dependence[i][j] > predictorDependence[i])
                {
                    predictorDependence[i] = dependence[i][j];
                }
            }
        }

        return predictorDependence;
    }

    protected double[][] getDependenceMatrix()
    {
        List<Integer> streamIds = pGetSortedKeys();
        double[][] dep = new double[streamIds.size()][streamIds.size()];

        for(int i=0; i < streamIds.size(); i++)
        {
            for(int j=i; j < streamIds.size(); j++)
            {
                if(i == j)
                {
                    dep[i][j] = 1.0;
                }
                else
                {
                    if(Config.LINEAR_DEPENDENCE)
                    {
                        dep[i][j] = DependenceEstimator.getLinearDependence(mStreams.get(streamIds.get(i)).getArray(),
                                                                            mStreams.get(streamIds.get(j)).getArray(),
                                                                            mStreams.get(streamIds.get(i)).getTimeArray(),
                                                                            mStreams.get(streamIds.get(j)).getTimeArray());
                    }
                    else
                    {
                        dep[i][j] = DependenceEstimator.getMonotonicDependence(mStreams.get(streamIds.get(i)).getArray(),
                                                                               mStreams.get(streamIds.get(j)).getArray(),
                                                                               mStreams.get(streamIds.get(i)).getTimeArray(),
                                                                               mStreams.get(streamIds.get(j)).getTimeArray());
                    }

                    dep[j][i] = dep[i][j];
                }
            }
        }

        return dep;
    }

    public RegModel getPredictiveModel(int streamIdY, int streamIdX)
    {
        final boolean useIntercept = true;
        RegModel sReg = new RegModel();
        sReg.setNoIntercept(false);

        double[] x = mStreams.get(streamIdX).getArray();
        double[] y = mStreams.get(streamIdY).getArray();

        final int len = (int) Math.max(x.length, y.length);

        if(len < 2)
        {
            return null;
        }

        double[] newx = DependenceEstimator.interpolate(x, mStreams.get(streamIdX).getTimeArray(), len);
        double[] newy = DependenceEstimator.interpolate(y, mStreams.get(streamIdY).getTimeArray(), len);

        Set<Double> ux = new HashSet<Double>();
        for(int i=0; i < newx.length; i++)
        {
            ux.add(newx[i]);
        }
        final int uniqueX = ux.size();
        if(uniqueX == 1) {
            return null;
        }

        double[][] REGX = new double[len][];

        for(int i=0; i < len; i++)
        {
            if(Config.LINEAR_DEPENDENCE || len < 4 || uniqueX < 4)
            {
                REGX[i] = new double[]{ newx[i] };
            }
            else
            {
                REGX[i] = new double[]{ newx[i],
                                        Math.pow(newx[i], 2.0),
                                        Math.pow(newx[i], 3.0) };
            }
        }
        sReg.newSampleData(newy, REGX);
        double[] param = sReg.estimateRegressionParameters();

        return sReg;
    }

    public RegModel[] getAllPredictiveModels(int[] predictors)
    {
        List<Integer> streamIds = pGetSortedKeys();
        RegModel[] regressions = new RegModel[predictors.length];
    
        int streamIdx = 0;
        for(Integer streamId : streamIds)
        {
            regressions[streamIdx] = getPredictiveModel(streamId, predictors[streamIdx]);
            streamIdx++;
        }
        return regressions;
    }

    public double[] generateVBounds()
    {
        List<Integer> streamIds = pGetSortedKeys();
        double[] values = new double[streamIds.size()];
        final double[] means = getStreamMeans();

        int streamIdx = 0;
        for(Integer streamId : streamIds)
        {
            double var = 0.0;
            double m4 = 0.0;
            double len = 0.0;
            final double mean = means[streamIdx];
            values[streamIdx] = 0.0;

            for(Double d : mStreams.get(streamId).mObservedValues)
            {
                var += Math.pow(mean - d, 2.0);
                m4  += Math.pow(mean - d, 4.0);
                len += 1.0;
            }
            var /= len;
            m4 /= len;

            if(len >= 2.0)
            {
                // normal approximation
                final double numSE = 1.0;
                values[streamIdx] = numSE * Math.sqrt( (2.0 * Math.pow(var,2.0)) / (len-1.0));
            }
            streamIdx++;
        }

        return values;
    }

    public double[] getExpectedConditionalVariance(int[] predictors)
    {
        List<Integer> streamIds = pGetSortedKeys();
        double[] values = new double[predictors.length];
        final double[] varianceY = getStreamVariances();

        int streamIdx = 0;
        for(Integer streamId : streamIds)
        {
            values[streamIdx] = 0.0;

            if(predictors[streamIdx] != -1)
            {
                final double varY = varianceY[streamIdx];
                if(Config.USE_MEAN_IMPUTATION)
                {
                    values[streamIdx] = varY;
                }
                else
                {
                    RegModel sReg = getPredictiveModel(streamId,
                                                       streamIds.get(predictors[streamIdx]));
                    if(sReg != null)
                    {
                        double rsquared = sReg.getSanatizedRSquared();

                        rsquared = Math.max(rsquared, 0.0);
                        if(Double.isNaN(rsquared)) rsquared = 0.0;

                        values[streamIdx] = varY - (varY * rsquared);
                    }
                }
            }
            streamIdx++;
        }
        return values;
    }

    protected long[] getObservedCountByStream()
    {
        List<Integer> streamIds = pGetSortedKeys();
        long[] observed = new long[streamIds.size()];
    
        int currentStreamIdx = 0;
        for(Integer streamId : streamIds)
        {
            observed[currentStreamIdx] = mStreams.get(streamId).mSummary.getN();
            currentStreamIdx++;
        }

        return observed;
    }

    public double[] getStreamMeans()
    {
        List<Integer> streamIds = pGetSortedKeys();
        double[] means = new double[streamIds.size()];

        int currentStreamIdx = 0;
        for(Integer streamId : streamIds)
        {
            means[currentStreamIdx] = mStreams.get(streamId).mSummary.getMean();
            currentStreamIdx++;
        }
        return means;
    }

    // Just for compatibility
    protected double[] getObservedCountByStreamDouble()
    {
        List<Integer> streamIds = pGetSortedKeys();
        double[] observed = new double[streamIds.size()];
    
        int currentStreamIdx = 0;
        for(Integer streamId : streamIds)
        {
            observed[currentStreamIdx] = mStreams.get(streamId).mSummary.getN();
            currentStreamIdx++;
        }

        return observed;
    }

    protected double[] getStreamVariances()
    {
        List<Integer> streamIds = pGetSortedKeys();
        double[] variance = new double[streamIds.size()];

        int currentStreamIdx = 0;
        for(Integer streamId : streamIds)
        {
            variance[currentStreamIdx] = 0.0;

            final double reportedVariance = mStreams.get(streamId).mSummary.getVariance();
            if(!Double.isNaN(reportedVariance))
            {
                variance[currentStreamIdx] = reportedVariance;
            }

            currentStreamIdx++;
        }

        return variance;
    }

    public List<Integer> getStreamIds()
    {
        return pGetSortedKeys();
    }

    protected List<Integer> pGetSortedKeys()
    {
        List<Integer> sortedKeys = new ArrayList<Integer>(mStreams.keySet());
        Collections.sort(sortedKeys);
        return sortedKeys;
    }

    protected void pInitializeStreamIfNull(Integer streamId)
    {
        if(!mStreams.containsKey(streamId))
        {
            mStreams.put(streamId, new StreamData());
        }
    }
}
