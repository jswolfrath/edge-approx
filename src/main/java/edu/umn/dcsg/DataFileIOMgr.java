package edu.umn.dcsg;

import edu.umn.dcsg.common.Config;
import edu.umn.dcsg.common.StreamTuple;
import edu.umn.dcsg.common.TraceInfo;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.random.JDKRandomGenerator;

public class DataFileIOMgr
{
    protected final int                 mDataIndex;
    protected final int                 mStreamIdIndex;
    protected final int                 mTimeIndex;
    protected final int                 mMetadataIndex;
    protected List<StreamTuple>         mStreamData;
    protected int                       mNextWindowStart;
    protected int                       mNextStreamId;
    protected int                       mTotalPoints;
    protected Map<Integer, Integer>     mStreamIdMapping;

    public DataFileIOMgr()
    {
        final TraceInfo traceInfo = Config.getTrace();

        mDataIndex = traceInfo.mValueIndex;
        mStreamIdIndex = traceInfo.mStreamIdIndex;
        mTimeIndex = traceInfo.mTimestampIndex;
        mMetadataIndex = traceInfo.mMetadataIndex;
        mTotalPoints = 0;

        mStreamData = new ArrayList<StreamTuple>();
        mNextWindowStart = 0;

        mStreamIdMapping = new HashMap<Integer, Integer>();
        mNextStreamId = 0;

        if(!pFileExists(traceInfo.mFileName))
        {
            throw new RuntimeException("Bad Input File in DataFileIOMgr");
        }

        try
        {
            pCacheFileContents(traceInfo.mFileName);
            mTotalPoints = mStreamData.size();
        }
        catch(IOException e)
        {
            throw new RuntimeException(e.toString());
        }
    }

    public int getTotalPoints()
    {
        return mTotalPoints;
    }

    public List<StreamTuple> getFullStream()
    {
        return mStreamData;
    }

    public List<StreamTuple> readWindow(long winLength)
    {
        final int samplesAvailable = mStreamData.size();
        List<StreamTuple> windowList = new ArrayList<StreamTuple>();

        if(mNextWindowStart >= samplesAvailable)
        {
            return windowList;  // Nothing to do
        }

        long curTime = mStreamData.get(mNextWindowStart).mTime;
        final long endTime = curTime + winLength;

        while(mNextWindowStart < samplesAvailable)
        {
            StreamTuple sample = mStreamData.get(mNextWindowStart);
            curTime = sample.mTime;

            if(curTime < endTime)
            {
                windowList.add(sample);
                mNextWindowStart++;
            }
            else
            {
                break;
            }
        }

        return windowList;
    }

    protected void pCacheFileContents(String filename) throws IOException
    {
        Reader reader = new FileReader(filename);
        Iterable<CSVRecord> records = CSVFormat.RFC4180
                                               .withFirstRecordAsHeader()
                                               .parse(reader);
        int row = 0;
        for (CSVRecord record : records)
        {
            row++;

            double value = Double.parseDouble(record.get(mDataIndex).trim());
            int providedStreamId = Integer.parseInt(record.get(mStreamIdIndex).trim());

            if(!mStreamIdMapping.containsKey(providedStreamId))
            {
                mStreamIdMapping.put(providedStreamId, providedStreamId);
                mNextStreamId++;
            }

            int streamId = providedStreamId;
            long time = Long.parseLong(record.get(mTimeIndex).trim());
            String metadata = "";
            if(mMetadataIndex >= 0)
            {
                metadata = record.get(mMetadataIndex).trim();
            }

            mStreamData.add(new StreamTuple(streamId, value, time, metadata));
        }

        reader.close();
    }

    protected boolean pFileExists(String filename)
    {
        File fileObj = new File(filename);
        return (fileObj.exists() || fileObj.isFile());
    }
}
