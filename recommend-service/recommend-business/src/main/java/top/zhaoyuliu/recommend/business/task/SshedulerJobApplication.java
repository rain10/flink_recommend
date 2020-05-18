package top.zhaoyuliu.recommend.business.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 
 * <一句话功能简述>
 * 
 * @Title: SshedulerJob.java
 * @Description: <功能详细描述>
 * @author Arain
 * @date 2020年5月15日下午12:14:34
 * @see [相关类/方法]
 * @since [产品/模块版本]
 */
public class SshedulerJobApplication {

	public static void main(String[] args) {
		ScheduledExecutorService pool = new ScheduledThreadPoolExecutor(1);
		// 每12小时调度一次
		pool.scheduleAtFixedRate(new Task(), 0, 12, TimeUnit.HOURS);

	}

	private static class Task implements Runnable {

		private Logger logger = LoggerFactory.getLogger(this.getClass());

		@Override
		public void run() {
			try {
			} catch (Exception e) {
				e.printStackTrace();
			}

			logger.info("调度完毕！");
		}
	}

}
