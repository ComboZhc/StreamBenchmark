package chao.cmu.capstone;

import java.util.Timer;
import java.util.TimerTask;

public class WindowValue extends TimerTask {
    private int[] values;
    private long[] times;
    private long sumTime;
    private long period;
    private Timer timer = new Timer();
    private int value;
    private long start;

    public WindowValue(int[] values, long[] times, long period) {
        this.values = values;
        this.times = times;
        this.sumTime = 0;
        this.period = period;
        for (long time : times)
            this.sumTime += time;
    }

    public int getValue() {
        return value;
    }

    public void start() {
        start = System.currentTimeMillis();
        value = values[0];
        timer.scheduleAtFixedRate(this, 0, period);
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
