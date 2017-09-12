package ElasticDocument;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import org.apache.log4j.Logger;

import java.util.concurrent.TimeUnit;


public class Profiler
{
    private static Logger logger = Logger.getLogger(Class.class.getName());
    private static final MetricRegistry metrics = new MetricRegistry();
    private static final Meter requestCount = metrics.meter("Requests sent");
    private static ConsoleReporter reporter;

    public static void start() {

        reporter = ConsoleReporter.forRegistry(metrics)
                .convertRatesTo(TimeUnit.SECONDS)
                .build();

        reporter.start(1, TimeUnit.SECONDS);
    }

    public static void requestSent(long numberOfRequests)
    {
        requestCount.mark(numberOfRequests);
    }

    public static void info(String message)
    {
        logger.info(message);
    }

    public static void error(String message)
    {
        logger.error(message);
    }

    public static void fatal(String message)
    {
        logger.fatal(message);
    }

    public static void close() {
        reporter.close();
    }
}
