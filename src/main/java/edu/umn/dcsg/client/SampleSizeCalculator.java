package edu.umn.dcsg.client;

import java.util.Arrays;
import java.util.Random;

public class SampleSizeCalculator
{
    public static int[] getProportionalAllocation(final double samplesAllowed, double[] popSizes)
    {
        final double totalSamples = (double) Arrays.stream(popSizes).sum();
        int sampleSizes[] = new int[popSizes.length];
        double props[] = new double[popSizes.length];
        double samplesAvail = Math.round(samplesAllowed);

        for(int i=0; i < popSizes.length; i++)
        {
            sampleSizes[i] = 1;
            samplesAvail -= 1.0;
            props[i] = popSizes[i] / totalSamples;
        }

        for(int i=0; i < popSizes.length; i++)
        {
            final double portion = samplesAvail * props[i];
            final int inc = (int) Math.round(portion);
            sampleSizes[i] += inc;
        }

        // Catch any rounding errors that cause us to under-estimate
        // the number of samples we should send.
        Random rng = new Random();
        double requested = (double) Arrays.stream(sampleSizes).sum();
        while(requested - samplesAllowed > 1.0)
        {
            int idx = rng.nextInt(popSizes.length);
            while(sampleSizes[idx] < 1.1)
            {
                idx = rng.nextInt(popSizes.length);
            }

            requested -= 1.0;
            sampleSizes[idx] -= 1.0;
        }

        return sampleSizes;
    }

    public static int[] getNeymanAllocation(double samplesAllowed, double[] popSizes, double[] variance)
    {
        assert(popSizes.length == variance.length);

        int sampleSizes[] = new int[variance.length];
        double[] weights = new double[variance.length];
        double allPopSum = Arrays.stream(popSizes).average().getAsDouble();

        double denom = 0.0;
        for(int i=0; i <popSizes.length; i++)
        {
            weights[i] = popSizes[i];
            denom += Math.sqrt(variance[i])*weights[i];
        }

        for(int i = 0; i < popSizes.length; i++)
        {
            final double sd = Math.sqrt(variance[i]);
            sampleSizes[i] = (int)Math.floor((samplesAllowed * ((weights[i]*sd)/ denom)));
            sampleSizes[i] = (int)Math.max(1.0, sampleSizes[i]);

            // Catch rounding errors
            if(sampleSizes[i] > popSizes[i])
            {
                sampleSizes[i] = (int) popSizes[i];
            }
        }

        return sampleSizes;
    }

    // This assumes that the base cost has already been considered and just deals
    // with the per sample cost for each population
    public static int[] getOptimalAllocation(double fixedCost, double[] popSizes,
                                             double[] variance, LinearCostFunction[] costs)
    {
        assert(popSizes.length == variance.length);
        assert(popSizes.length == costs.length);

        int sampleSizes[] = new int[variance.length];
        double upFrontCost = 0.0;
        for(int i=0; i < variance.length; i++)
        {
            sampleSizes[i] = 1;
            upFrontCost += costs[i].getCostPerSample();
        }

        double globalDenom = 0.0;
        double globalNum = 0.0;
        for(int i=0; i <popSizes.length; i++)
        {
            final double sd = Math.sqrt(variance[i]);
            final double sqrtCost = Math.sqrt(costs[i].getCostPerSample());

            globalNum += popSizes[i] * sd / sqrtCost;
            globalDenom += popSizes[i] * sd * sqrtCost;
        }

        final double n = ((fixedCost - upFrontCost) * globalNum) / globalDenom;

        for(int i = 0; i < popSizes.length; i++)
        {
            final double sd = Math.sqrt(variance[i]);
            final double sqrtCost = Math.sqrt(costs[i].getCostPerSample());

            final double localNum = popSizes[i] * sd / sqrtCost;

            sampleSizes[i] += (int)Math.round((n * localNum) / globalNum);
        }

        //
        // FIXUP ANY OVERCOMMITED STRATA
        //
        for(int i=0; i < popSizes.length; i++)
        {
            int fixRetries = 0;
            final int maxRetries = 1000;
            Random rng = new Random();

            while(sampleSizes[i] > (int)popSizes[i])
            {
                if(fixRetries == maxRetries) break;

                int incIdx = rng.nextInt(popSizes.length);
                if(incIdx != i && sampleSizes[incIdx] < popSizes[incIdx])
                {
                    sampleSizes[incIdx]++;
                    sampleSizes[i]--;
                }
                else
                    fixRetries++;
            }

            if(fixRetries == maxRetries)
            {
                System.err.println("Can't fix overcommitted stratum");
                sampleSizes[i] = (int)popSizes[i];
            }
        }

        return sampleSizes;
    }
}
