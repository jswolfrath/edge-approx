package edu.umn.dcsg.common;

// Note: This design is rather rigid, since there is no runtime configuration.
// A future enhancement would be to configure behavior dynamically based on an
// input file or something similar.
public class Config
{
    /// Strategies
    public static final int SRS_STRATEGY                = 0x01;
    public static final int STRATIFIED_EVEN_STRATEGY    = 0x02;
    public static final int NEYMAN_STRATEGY             = 0x03;
    public static final int IMPUTATION_STRATEGY         = 0x04;

    public static final TraceInfo getTrace()
    {
        return TraceInfo.MVNORM;
    }

    public static boolean LINEAR_DEPENDENCE = true;

    public static boolean USE_MEAN_IMPUTATION = false;

    public static double SAMPLING_FRACTION = 0.25;

    public static int STRATEGY = Config.SRS_STRATEGY;

    public static boolean USE_THINNING = false;

    public static String getStrategyString()
    {
        if(Config.STRATEGY == Config.IMPUTATION_STRATEGY)
            return "IMPUTATION";

        else if(Config.STRATEGY == Config.NEYMAN_STRATEGY)
            return "NEYMAN";

        else if(Config.STRATEGY == Config.STRATIFIED_EVEN_STRATEGY)
            return "STRATIFIED";

        else if(Config.STRATEGY == Config.SRS_STRATEGY)
            return "SRS";

        return "UNKNOWN";
    }

    static String getDatasetString()
    {
        return getTrace().mId;
    }

    static String getDependenceType()
    {
        if(Config.LINEAR_DEPENDENCE)
            return "LINEAR";
        return "CUBIC";
    }

    static void print()
    {
        System.out.println("============================================");
        System.out.println("Configuration");
        System.out.println("============================================");
        System.out.println("Dataset:            " + getDatasetString());
        System.out.println("Strategy:           " + getStrategyString());
        System.out.println("Sampling Fraction:  " + Config.SAMPLING_FRACTION);
        System.out.println("Dependence:         " + getDependenceType());
        System.out.println("Thinning:           " + Config.USE_THINNING);
        System.out.println("============================================");
    }
}
