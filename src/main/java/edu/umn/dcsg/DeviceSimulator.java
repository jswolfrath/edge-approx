package edu.umn.dcsg;

import edu.umn.dcsg.common.Config;
import edu.umn.dcsg.common.GenericPayload;
import edu.umn.dcsg.common.TraceInfo;
import edu.umn.dcsg.common.StreamTuple;

import edu.umn.dcsg.client.EdgeSimulator;

import edu.umn.dcsg.server.CloudSimulator;
import edu.umn.dcsg.server.SimulationResults;

import java.util.ArrayList;
import java.util.List;

public class DeviceSimulator 
{
    private int                 mSamplesGenerated;
    private DataFileIOMgr       mDataMgr;
    private List<StreamTuple>   mWindowData;
    private int                 mWindowNumber;

    public DeviceSimulator()
    {
        mSamplesGenerated = 0;
        mWindowNumber = 0;
        mWindowData = new ArrayList<StreamTuple>();
        mDataMgr = new DataFileIOMgr();

        pReadWindow();
    }

    public int getTotalPoints()
    {
        return mDataMgr.getTotalPoints(); 
    }

    public int getWindowNumber()
    {
        return mWindowNumber;
    }

    public List<GenericPayload> nextTuple()
    {
        final int winDuration = Config.getTrace().mWindowDuration;
        final int numWindows = Config.getTrace().mNumWindows;

        List<GenericPayload> payloads = new ArrayList<GenericPayload>();

        if(mSamplesGenerated == 0)
        {
            payloads.add(new GenericPayload(GenericPayload.TYPE_HEADER));
        }

        if(mSamplesGenerated < mWindowData.size())
        {
            GenericPayload payload = new GenericPayload(GenericPayload.TYPE_TUPLE);
            StreamTuple dataPoint = mWindowData.get(mSamplesGenerated);

            payload.setStreamId(dataPoint.mStreamId.toString());
            payload.setValue(Double.toString(dataPoint.mValue));
            payload.setTime(Long.toString(dataPoint.mTime % winDuration));
            payloads.add(payload);

            mSamplesGenerated++;
        }
        else if(mSamplesGenerated == mWindowData.size())
        {
            payloads.add(new GenericPayload(GenericPayload.TYPE_TRAILER));
            mSamplesGenerated = 0;

            if(!pReadWindow() || mWindowNumber > numWindows)
            {
                // We're done here.
                mSamplesGenerated = winDuration + 1;
                mWindowData.clear();
            }
        }

        return payloads;
    }

    protected boolean pReadWindow()
    {
        final int winDuration = Config.getTrace().mWindowDuration;

        mWindowData.clear();
        mWindowData = mDataMgr.readWindow(winDuration);
        mWindowNumber++;

        return (mWindowData.size() != 0);
    }
}