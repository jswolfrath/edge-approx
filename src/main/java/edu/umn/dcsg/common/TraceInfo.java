package edu.umn.dcsg.common;

public class TraceInfo
{
    public final static TraceInfo MVNORM    = new TraceInfo(
        "MVNORM",
        "./data/mvnorm.csv",
        2 * 60,  // 2 minutes
        60 / 2,  // Run against 1 hour of data
        2, 0, 1, // Specific to the input file
        -1       // No additional metadata
    );

    public String   mId;
    public String   mFileName;
    public Integer  mWindowDuration;
    public Integer  mNumWindows;
    public Integer  mValueIndex;
    public Integer  mStreamIdIndex;
    public Integer  mTimestampIndex;
    public Integer  mMetadataIndex;
    
    public TraceInfo(final String id,
                     final String file,
                     final Integer winDuration,
                     final Integer numWindows,
                     final Integer valueIndex,
                     final Integer streamIndex,
                     final Integer timeIndex,
                     final Integer metadataIndex)
    {
        mId = id;
        mFileName = file;
        mWindowDuration = winDuration;
        mNumWindows = numWindows;
        mValueIndex = valueIndex;
        mStreamIdIndex = streamIndex;
        mTimestampIndex = timeIndex;
        mMetadataIndex = metadataIndex;
    }

    public void print()
    {
        System.out.println("Trace Info:");
        System.out.println("  Id:                        " + mId);
        System.out.println("  File:                      " + mFileName);
        System.out.println("  Window Duration in (s):    " + mWindowDuration);
        System.out.println("  Num Windows in Experiment: " + mNumWindows);
    }
}

