package edu.umn.dcsg;

import edu.umn.dcsg.DeviceSimulator;
import edu.umn.dcsg.common.Config;
import edu.umn.dcsg.common.GenericPayload;
import edu.umn.dcsg.common.TraceInfo;
import edu.umn.dcsg.common.StreamTuple;

import edu.umn.dcsg.client.EdgeSimulator;

import edu.umn.dcsg.server.CloudSimulator;
import edu.umn.dcsg.server.SimulationResults;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

public class AppTest 
{
    @Test
    public void simulationTest()
    {
        /// I understand this is not what unit tests are for.
        /// Just using this for convenience in maven to execute a run.
        
        CloudSimulator cloud = new CloudSimulator();
        EdgeSimulator edge = new EdgeSimulator(cloud);
        DeviceSimulator devSimulator = new DeviceSimulator();

        List<GenericPayload> data = devSimulator.nextTuple();
        while(data.size() > 0)
        {
            for(GenericPayload payload : data)
            {
                edge.handleEvent(payload);
            }

            data = devSimulator.nextTuple();
        }

        SimulationResults res = cloud.getResults();

        final double avgLoss = res.getAVGNormalizedRMSEAcrossAllStreams();
        final double varLoss = res.getVARNormalizedRMSEAcrossAllStreams();
        final double minLoss = res.getMINNormalizedRMSEAcrossAllStreams();
        final double maxLoss = res.getMAXNormalizedRMSEAcrossAllStreams();

        final String strategy = Config.getStrategyString();
        final double sf = Config.SAMPLING_FRACTION;

        System.out.println(strategy + " num windows: " + devSimulator.getWindowNumber());
        System.out.println(strategy + " avg loss " + sf + ": " + avgLoss);
        System.out.println(strategy + " var loss " + sf + ": " + varLoss);
        System.out.println(strategy + " min loss " + sf + ": " + minLoss);
        System.out.println(strategy + " max loss " + sf + ": " + maxLoss);

        System.out.println("ALLOWED COST: " + edge.getAllowedCost());
    }
}
