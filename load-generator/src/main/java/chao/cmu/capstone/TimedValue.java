package chao.cmu.capstone;

import java.util.Timer;
import java.util.TimerTask;

public class TimedValue extends TimerTask {
    private int[] values;
    private long[] times;
    private long sumTime;
    private Timer timer = new Timer();
    private int value;
    private long start;

    public TimedValue(int[] values, long[] times) {
        this.values = values;
        this.times = times;
        this.sumTime = 0;
        for (long time : times)
            this.sumTime += time;
    }

    public int getValue() {
        return value;
    }

    public void start() {
        start = System.currentTimeMillis();
        value = values[0];
        timer.scheduleAtFixedRate(this, 0, 100);
    }

    @Override
    public void run() {
        long elapsed = (System.currentTimeMillis() - start) % sumTime;
        int index = 0;
        while (elapsed >= times[index]) {
            elapsed -= times[index];
            index++;
        }
        int currValue = values[index];
        int nextValue = values[(index + 1) % values.length];
        value = currValue + (int)((nextValue - currValue) * elapsed / times[index]);
    }
}
