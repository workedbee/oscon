package com.dataartisans;

import com.dataartisans.data.DataPoint;
import com.dataartisans.sources.TimestampSource;

/**
 * This timestamp source generates data for specified time (in ms) and then stops
 */
public class LimitedTimestampSource extends TimestampSource {
    private static final int SLOWDOWN_FACTOR = 1;
    private static final int PERIOD_MS = 100;

    private final int limitedTimeMs;

    public LimitedTimestampSource(int limitedTimeMs) {
        super(PERIOD_MS, SLOWDOWN_FACTOR);
        this.limitedTimeMs = limitedTimeMs;
    }

    @Override
    public void run(SourceContext<DataPoint<Long>> ctx) throws Exception {
        long startTime = System.currentTimeMillis();
        while (isRunning()
                && startTime + limitedTimeMs > System.currentTimeMillis()) {
            doCycle(ctx);
        }
    }
}
