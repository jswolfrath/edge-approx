package edu.umn.dcsg.common;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

// This is a generic class that represents a payload that gets sent
// over the network. We derive from the org.apache.storm.tuple.Values
// class, which allows us to easily send this in a storm cluster or
// simply over the wide-area network.
public class GenericPayload extends Values
{
    public final static int TYPE_INVALID            = 0x00;
    public final static int TYPE_TUPLE              = 0x01;
    public final static int TYPE_IMPUTATION_MODEL   = 0x02;
    public final static int TYPE_HEADER             = 0x03;
    public final static int TYPE_TRAILER            = 0x04;
    public final static int MAX_VALID_TYPE          = 0x05;

    private static final long serialVersionUID = 1L;

    protected final static String TYPE_FIELD_NAME       = "TYPE";
    protected final static String STREAM_ID_FIELD_NAME  = "STREAM_ID";
    protected final static String VALUE_FIELD_NAME      = "VALUE";
    protected final static String TIMESTAMP_FIELD_NAME  = "TIME";
    protected final static String METADATA_FIELD_NAME   = "METADATA";

    protected final static int TYPE_IDX         = 0;
    protected final static int STREAM_ID_IDX    = 1;
    protected final static int VALUE_IDX        = 2;
    protected final static int TIME_IDX         = 3;
    protected final static int METADATA_IDX     = 4;
    protected final static int LIST_SIZE        = 5;

    public GenericPayload(final Integer type)
    {
        assert(type != TYPE_INVALID);
        assert(type <= MAX_VALID_TYPE);

        // Initialize the fields to empty strings
        for(int i=0; i < LIST_SIZE; i++)
        {
            add("");
        }

        set(TYPE_IDX, type.toString());
        set(TIME_IDX, "0");
        assert(size() == LIST_SIZE);
    }

    public GenericPayload(org.apache.storm.tuple.Tuple tuple)
    {
        assert(tuple.size() == LIST_SIZE);

        add(TYPE_IDX, tuple.getValueByField(TYPE_FIELD_NAME));
        add(STREAM_ID_IDX, tuple.getValueByField(STREAM_ID_FIELD_NAME));
        add(VALUE_IDX, tuple.getValueByField(VALUE_FIELD_NAME));
        add(TIME_IDX, tuple.getValueByField(TIMESTAMP_FIELD_NAME));
        add(METADATA_IDX, tuple.getValueByField(METADATA_FIELD_NAME));
    }

    public void setStreamId(String streamId)
    {
        set(STREAM_ID_IDX, streamId);
    }

    public String getStreamId()
    {
        return (String)get(STREAM_ID_IDX);
    }

    public void setValue(String value)
    {
        set(VALUE_IDX, value);
    }

    public String getValue()
    {
        return (String) get(VALUE_IDX);
    }

    public void setTime(String time)
    {
        set(TIME_IDX, time);
    }

    public String getTime()
    {
        return (String) get(TIME_IDX);
    }

    public void setMetadata(String metadata)
    {
        set(METADATA_IDX, metadata);
    }

    public String getMetadata()
    {
        return (String) get(METADATA_IDX);
    }

    public static Fields getOutputFields()
    {
        return new Fields(TYPE_FIELD_NAME,
                          STREAM_ID_FIELD_NAME,
                          VALUE_FIELD_NAME,
                          TIMESTAMP_FIELD_NAME,
                          METADATA_FIELD_NAME);
    }

    public boolean isTuple()
    {
        return Integer.parseInt((String) get(TYPE_IDX)) == TYPE_TUPLE;
    }

    public boolean isImputationModel()
    {
        return Integer.parseInt((String) get(TYPE_IDX)) == TYPE_IMPUTATION_MODEL;
    }

    public boolean isHeader()
    {
        return Integer.parseInt((String) get(TYPE_IDX)) == TYPE_HEADER;
    }

    public boolean isTrailer()
    {
        return Integer.parseInt((String) get(TYPE_IDX)) == TYPE_TRAILER;
    }
}
