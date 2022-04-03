package edu.umn.dcsg.common;

// Class that represents a stream tuple
// (as opposed to a Storm tuple object)
public class StreamTuple
{
    public Integer  mStreamId;
    public Double   mValue;
    public Long     mTime;
    public String   mMetadata;

    public StreamTuple(final Integer streamId, final Double value) 
    {
        mStreamId = streamId;
        mValue = value;
        mTime = (long)0;
        mMetadata = new String();
    }

    public StreamTuple(final Integer streamId,
                       final Double value,
                       final Long time)
    {
        mStreamId = streamId;
        mValue = value;
        mTime = time;
        mMetadata = new String();
    }

    public StreamTuple(final Integer streamId,
                       final Double value,
                       final Long time,
                       final String metadata)
    {
        mStreamId = streamId;
        mValue = value;
        mTime = time;
        mMetadata = metadata;
    }

    public String toString()
    {
        return "StreamTuple: " + mStreamId + " " + mTime + " " + mValue;
    }
}
