package com.fxdayu.client.IO;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import javax.net.ssl.ExtendedSSLSession;

import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.fxdayu.client.Config;
import com.fxdayu.client.utils;
import com.rabbitmq.client.AMQP.Basic;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.impl.Method;

public class MQManager {
	
	private ConnectionFactory factory;
	private Connection connection;
	private Channel channel;
	private Logger logger;
	private static BasicProperties.Builder propsBuilder = new BasicProperties.Builder();
	
	public static final String EXCHANGE = "Tick";
		
	public static void main(String[] args) throws IOException, TimeoutException{
//		MQManager manager = new MQManager();
//		manager.sendExchange("000001.XSHG", 00, 00, new JSONObject());
	}
	
	public MQManager() throws IOException, TimeoutException{
		factory = new ConnectionFactory();
		factory.setHost("localhost");
		connection = factory.newConnection();
		channel = connection.createChannel();
		create();
		
	}
	
	public MQManager(JSONObject mqConfig) throws IOException, TimeoutException{
		initFactory(mqConfig);
		connection = factory.newConnection();
		channel = connection.createChannel();
		create();
		
	}
	
	private void initFactory(JSONObject mqConfig){
		factory = new ConnectionFactory();
		factory.setHost(mqConfig.getString("host"));
		if (mqConfig.has("username")) {
			factory.setUsername(mqConfig.getString("username"));
		}
		if (mqConfig.has("password")) {
			factory.setPassword(mqConfig.getString("password"));
		}
	}	
	
	public BasicProperties buildProperties(String code){
		Map<String, Object> headers = new HashMap<>();
		headers.put("code", code);
		return propsBuilder.headers(headers).build();
	}
	
	public void setLogger(Logger logger){
		this.logger = logger;
	}
	
	public void create(){
		try {
			channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.FANOUT);
		} catch (IOException e) {
			
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error("Exchange declair fail, exit service", e);
			System.exit(0);
		}
	}
	
	public void sendExchange(String code, int date, int time, JSONObject priceLevels){
		String unfolded = utils.unfold(code);
		priceLevels.put("code", unfolded);
		priceLevels.put("date", date);
		priceLevels.put("time", time);
		try {
			channel.basicPublish(EXCHANGE, "", buildProperties(unfolded), priceLevels.toString().getBytes());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			logger.error(String.format("Send message %s fail", priceLevels), e);
		}
	}
	
}
