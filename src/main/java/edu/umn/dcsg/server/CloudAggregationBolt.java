package edu.umn.dcsg.server;

import edu.umn.dcsg.common.GenericPayload;
import edu.umn.dcsg.common.StreamTuple;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.List;
import java.util.Map;

public class CloudAggregationBolt extends AbstractCloud implements IRichBolt
{
    protected OutputCollector mCollector;

    public CloudAggregationBolt()
    {
        super();
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector)
    {
        this.mCollector = collector;
    }
 
    @Override
    public void execute(Tuple tuple)
    {
        GenericPayload payload = new GenericPayload(tuple);
        pHandleTuple(payload);

        mCollector.ack(tuple);
    }

    @Override
    public void cleanup()
    {
    }
 
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
       declarer.declare(new Fields());
    }
     
    @Override
    public Map<String, Object> getComponentConfiguration() {
       return null;
    }

    protected void pAnalyzeSamples(List<StreamTuple> trueAggregates)
    {
        // no-op
    }
}