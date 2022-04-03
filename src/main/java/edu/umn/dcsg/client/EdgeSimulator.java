package edu.umn.dcsg.client;

import edu.umn.dcsg.common.GenericPayload;
import edu.umn.dcsg.common.SimulationHandler;

public class EdgeSimulator extends AbstractEdge implements SimulationHandler
{
    private static final long serialVersionUID = 1L;

    protected SimulationHandler mUpstreamHandler;

    public EdgeSimulator(SimulationHandler upstreamHandler)
    {
        super();
        mUpstreamHandler = upstreamHandler;
    }

    // Simulator function for input data. Storm has execute() function.
    public void handleEvent(final GenericPayload payload)
    {
        pHandleTuple(payload);
    }

    protected void pHandleTuple(final GenericPayload gp)
    {
        if(gp.isTuple())
        {
            pInsertValue(gp);
        }
        else if(gp.isTrailer())
        {
            flush();
        }
    }

    protected void pSendPayload(final GenericPayload gp)
    {
        mUpstreamHandler.handleEvent(gp);
    }
}
