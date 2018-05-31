package com.kd.mq;

import java.io.File;

import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.kd.server.config.FileResourceLoader;
import com.kd.server.config.IConfig;
import com.kd.server.config.IResourceLoader;
import com.kd.server.config.ResourceLoaderConfig;

public class MQProducer {
	static String configPath = System.getProperty("moquette.path", null);
	static File defaultConfigurationFile = new File(configPath, IConfig.DEFAULT_CONFIG);
	static IResourceLoader filesystemLoader = new FileResourceLoader(defaultConfigurationFile);
	static IConfig config = new ResourceLoaderConfig(filesystemLoader);

	public static void produce(String producerGroupName, String producerName, String topic, String tag, String key,
			byte[] body) {
		/**
		 * 一个应用创建一个Producer，由应用来维护此对象，可以设置为全局对象或者单例<br>
		 * 注意：ProducerGroupName需要由应用来保证唯一<br>
		 * ProducerGroup这个概念发送普通的消息时，作用不大，但是发送分布式事务消息时，比较关键，
		 * 因为服务器会回查这个Group下的任意一个Producer
		 */
		final DefaultMQProducer producer = new DefaultMQProducer(producerGroupName);
		producer.setNamesrvAddr(config.getProperty("deshost"));
		producer.setInstanceName(producerName);

		/**
		 * Producer对象在使用之前必须要调用start初始化，初始化一次即可<br>
		 * 注意：切记不可以在每次发送消息时，都调用start方法
		 */
		try {
			producer.start();
		} catch (MQClientException e1) {
			System.out.println("生产者启动失败");
			e1.printStackTrace();
		}

		/**
		 * 下面这段代码表明一个Producer对象可以发送多个topic，多个tag的消息。
		 * 注意：send方法是同步调用，只要不抛异常就标识成功。但是发送成功也可会有多种状态，<br>
		 * 例如消息写入Master成功，但是Slave不成功，这种情况消息属于成功，但是对于个别应用如果对消息可靠性要求极高，<br>
		 * 需要对这种情况做处理。另外，消息可能会存在发送失败的情况，失败重试由应用来处理。
		 */
		for (int i = 0; i < 1; i++) {
			try {
				Message msg = new Message(topic, tag, key, body);
				SendResult sendResult = producer.send(msg);
				System.out.println(sendResult);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		/**
		 * 应用退出时，要调用shutdown来清理资源，关闭网络连接，从MetaQ服务器上注销自己
		 * 注意：我们建议应用在JBOSS、Tomcat等容器的退出钩子里调用shutdown方法
		 */
		producer.shutdown();
		// Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
		// public void run() {
		// producer.shutdown();
		// }
		// }));
		System.exit(0);
	}
	
	public static void main(String[] args) throws MQClientException, InterruptedException {
		produce("producerGroupName", "producerName", "topic", "tag", "key", "body".getBytes());
	}
}