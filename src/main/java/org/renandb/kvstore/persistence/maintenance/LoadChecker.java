package org.renandb.kvstore.persistence.maintenance;

public class LoadChecker {

    private static final long QPM_UPDATE_INTERVAL_MILLIS = 5000;
    private static int HIGH_LOAD_QPM = 14; // Queries per minute

    private long lastCheckTimestamp = 0;
    private int numCalls = 0;

    private int lastQPM = 0;

    public void notifyUsage(){
        if(lastCheckTimestamp == 0) lastCheckTimestamp = System.currentTimeMillis();
        long now = System.currentTimeMillis();
        int timePassed = (int) (now - lastCheckTimestamp);
        synchronized(this) {
            numCalls++;
            if (timePassed >= QPM_UPDATE_INTERVAL_MILLIS) {
                lastQPM = (timePassed/ 1000) / (numCalls * 60);
                numCalls = 0;
                lastCheckTimestamp = now;
            }
        }
    }

    public int getWaitTimeForCurrentLoad(int maxWaitMillis){
        if(lastQPM >= HIGH_LOAD_QPM) return maxWaitMillis;
        return (lastQPM / HIGH_LOAD_QPM) * maxWaitMillis;
    }

}
