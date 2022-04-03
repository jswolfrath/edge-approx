package edu.umn.dcsg.client;

import edu.umn.dcsg.common.GenericPayload;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;

public class EdgeBolt extends AbstractEdge implements IRichBolt
{
    final int WINDOW_DURATION_IN_MS = 2 * 60 * 1000;    // 2 minutes

    class TTLCacheFlushTask extends TimerTask {

        protected EdgeBolt mBolt;

        public TTLCacheFlushTask(EdgeBolt bolt) {
            mBolt = bolt;
        }

        public void run() {
            mBolt.flush();
        }
    }

    protected Timer mWindowTimer;

    // Create instance for OutputCollector which collects and emits tuples to
    // produce output
    private OutputCollector mCollector;

    public EdgeBolt()
    {
        super();
        mWindowTimer = new Timer();
        mWindowTimer.schedule(new TTLCacheFlushTask(this), WINDOW_DURATION_IN_MS);
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector)
    {
        mCollector = collector;
    }

    @Override
    public void execute(Tuple tuple)
    {
        GenericPayload payload = new GenericPayload(tuple);
        pHandleTuple(payload);
    }

    protected void pHandleTuple(final GenericPayload gp)
    {
        if(gp.isTuple())
        {
            pInsertValue(gp);
        }
    }

    @Override
    public void cleanup() {}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(GenericPayload.getOutputFields());
    }
        
    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    protected void pSendPayload(final GenericPayload gp)
    {
        mCollector.emit(gp);
    }
}