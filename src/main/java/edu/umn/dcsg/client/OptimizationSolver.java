package edu.umn.dcsg.client;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.util.Arrays;

public class OptimizationSolver
{
    protected OptimizationParms mParms;

    public OptimizationSolver(OptimizationParms parms)
    {
        mParms = parms;
    }

    public double[] compute(final double budget) throws Exception
    {
        Process process = Runtime.getRuntime()
                                 .exec(new String[]{ "python",
                                                     "./src/main/py/opt.py",
                                                     mParms.toString()
                                                   });

        String stdOutLine = null;
        BufferedReader stdOut = new BufferedReader(
                                    new InputStreamReader(process.getInputStream()));
        String optVal = "";
        while (null != (stdOutLine = stdOut.readLine()))
        {
            optVal += stdOutLine.strip();
        }
        process.waitFor();

        String[] items = optVal.replaceAll("\\[", "")
                               .replaceAll("\\]", "")
                               .replaceAll("\\s", "")
                               .split(",");

        double[] result = new double[items.length];
        for(int j=0; j < result.length; j++) result[j] = 0.0;

        for (int i = 0; i < items.length; i++)
        {
            try
            {
                result[i] = Double.parseDouble(items[i]);
            } 
            catch (NumberFormatException nfe)
            {
                System.err.println("Failed to parse python output");
                System.err.println(mParms.toString());
                System.err.println("Output: " + optVal);
                //System.exit(0);

                // Gross, but keep moving for now
                double[] ss = new double[mParms.mStrataSizes.length];
                LinearCostFunction[] costs = new LinearCostFunction[mParms.mStrataSizes.length];
                for(int j=0; j < ss.length; j++) {
                    ss[j] = (double)mParms.mStrataSizes[j];
                    costs[j] = new LinearCostFunction(0, mParms.mStrataCosts[j]);
                }

                int[] tmp = SampleSizeCalculator.getOptimalAllocation(mParms.mSamplesAllowed, ss, 
                                                                      mParms.mStrataVariance, costs);
                for(int j=0; j < tmp.length; j++) result[j] = (double)tmp[j];
                break;
            }
        }

        return result;
    }
}
