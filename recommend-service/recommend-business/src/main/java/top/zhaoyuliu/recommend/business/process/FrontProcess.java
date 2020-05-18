package top.zhaoyuliu.recommend.business.process;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import top.zhaoyuliu.recommend.core.model.Metrics;
import top.zhaoyuliu.recommend.core.model.Rating;
/**  
 * Copyright © 2020 Arain. All rights reserved.
 *
 * @Title: FrontProcess.java
 * @Prject: recommend-business
 * @Package: top.zhaoyuliu.recommend.business.process
 * @Description: <功能详细描述>
 * @author: Arain  
 * @date: 2020年5月14日 下午3:29:11
 * @version: V1.0  
 */
public class FrontProcess extends ProcessFunction<Metrics, Rating>{

	/**
	 * serialVersionUID
	 */
	private static final long serialVersionUID = 1L;
	
//	 private ValueState<CountWithTimestamp> state;
	
	 @Override
	    public void open(Configuration parameters) throws Exception {
//	        state = getRuntimeContext().getState(new ValueStateDescriptor<>("myState", CountWithTimestamp.class));
	    }

	@Override
	public void processElement(Metrics arg0, ProcessFunction<Metrics, Rating>.Context arg1, Collector<Rating> arg2)
			throws Exception {
		
	}
	
	@Override
	public void onTimer(long timestamp, ProcessFunction<Metrics, Rating>.OnTimerContext ctx, Collector<Rating> out)
			throws Exception {
		super.onTimer(timestamp, ctx, out);
	}
}
