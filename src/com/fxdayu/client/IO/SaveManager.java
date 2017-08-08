package com.fxdayu.client.IO;

import java.awt.print.Printable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;


import org.apache.log4j.Logger;
import org.json.JSONObject;

import com.fxdayu.client.CandleInstance;
import com.fxdayu.client.Config;
import com.fxdayu.client.enums.WriteQuote;


public class SaveManager {
	
	HashMap<String, CandleInstance> instances;
	RedisHandler handler;
	public static String[] candleColumns = {"datetime", "open", "high", "low", "close", "volume"};
	private Logger logger;
	
	private static final SimpleDateFormat TimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	
	private static final int START = 93000000;
	private static final int END = 150000000;
//	private int latest;
	
	public SaveManager(){
		instances = new HashMap<>();
		handler = new RedisHandler();
	}
	
	public SaveManager(JSONObject redisConfig){
		instances = new HashMap<>();
		handler = new RedisHandler(redisConfig);
	}
	
	public SaveManager(Config config){
		instances = new HashMap<>();
		handler = new RedisHandler(config.getRedisConfig());
		logger = config.logger;
		restore();
	}
	
	public void setLogger(Logger logger){
		this.logger = logger;
	}
	
	public void restore(){
		for (String string : handler.jedis.keys("*.XSH*:datetime")) {
			try {
				String code = string.substring(0, 11);
				String securityID = string.substring(0, 6);
				instances.put(securityID, restoreCandle(code));
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				logger.error(String.format("Restore %s failed", string), e);
			}
		}

	}
	
	public static void main(String[] args) throws ParseException{
		SaveManager manager = new SaveManager();
		System.out.println(manager.restoreCandle("000001.XSHE"));
		
	}
	
	public CandleInstance restoreCandle(String securityID) throws ParseException{
		List<Object> latest = handler.getLatest(securityID, candleColumns);
		
		CandleInstance candle = new CandleInstance(
				securityID,
				TimeFormat.parse((String) latest.get(0)), 
				Float.valueOf((String) latest.get(1)),
				Float.valueOf((String) latest.get(2)),
				Float.valueOf((String) latest.get(3)),
				Float.valueOf((String) latest.get(4)),
				handler.jedis.lrange(handler.getColume(securityID, "volume"), 0, -1)
				);
		return candle;
	}
	
	public CandleInstance createCandle(String security, Date time, int price, long volume){
		CandleInstance instance = new CandleInstance(security, time, price, volume);
		instances.put(security, instance);
		handler.insertCandle(
				instance.code, 
				instance.getEndTime(), 
				instance.strOpen(), 
				instance.strHigh(), 
				instance.strLow(), 
				instance.strClose(), 
				instance.strVolume());
		Date ddl = (Date) time.clone();;
		ddl.setHours(18);
		ddl.setMinutes(0);
		handler.expireAt(instance.code, candleColumns, ddl.getTime()/1000);
		return instance;
	}
	
	public void onQuote(String security, Date time, int price, long volume){
		CandleInstance instance = instances.get(security);
		
		if (instance == null) {
			instance = createCandle(security, time, price, volume);
		}
		
		
		
		WriteQuote quote = instance.onQuote(time, price, volume);
		
		switch (quote) {
		case UPDATE:
			handler.updateCandle(instance.code, instance.strOpen(), instance.strHigh(), instance.strLow(), instance.strClose(), instance.strVolume(), -1);
			break;
		case INSERT:
			handler.insertCandle(instance.code, instance.getEndTime(), instance.strOpen(), instance.strHigh(), instance.strLow(), instance.strClose(), instance.strVolume());
			break;
		case ERROR:
			return;
		default:
			break;
		}
		
	}
}
