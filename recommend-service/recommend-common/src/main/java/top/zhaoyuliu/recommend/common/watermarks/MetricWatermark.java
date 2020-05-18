package top.zhaoyuliu.recommend.common.watermarks;

import javax.annotation.Nullable;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import top.zhaoyuliu.recommend.core.model.Metrics;

public class MetricWatermark implements AssignerWithPeriodicWatermarks<Metrics> {

	private static final long serialVersionUID = -4288282063022616906L;
	private long currentTimestamp = Long.MIN_VALUE;

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - 1);
    }

    @Override
    public long extractTimestamp(Metrics metrics, long l) {
        long timestamp = metrics.getTimestamp();
        this.currentTimestamp = timestamp;
        return timestamp;
    }
}
