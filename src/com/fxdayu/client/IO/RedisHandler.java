package com.fxdayu.client.IO;

import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.quant360.api.model.mds.MdsPriceLevel;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;

public class RedisHandler {
	public Jedis jedis;
	JedisPool pool;
	JSONObject config;
	JedisPoolConfig poolConfig;
	private Logger logger;
	
	public static void main(String[] args){
		RedisHandler handler = new RedisHandler();
		Jedis jedis = handler.jedis;
	}
	
	public void setConfig(JSONObject config){
		this.config = config;
	}
	
	public RedisHandler(JSONObject conifg){
		setConfig(conifg);
		pool = getPool();
		jedis = pool.getResource();
	}
	
	public RedisHandler(){
		pool = getPool();
		jedis = pool.getResource();
	}
	
	public JedisPool getPool(){
		if(config != null){
			if (config.has("password")) {
				return new JedisPool(
						new JedisPoolConfig(), 
						this.config.getString("host"), 
						this.config.getInt("port"), 
						0, 
						this.config.getString("password")
						);
			}else{
				return new JedisPool(this.config.getString("host"), this.config.getInt("port"));
			}
		}else{
			return new JedisPool();
		}
	}
	
	public Jedis getRedisClient(){
		if(jedis != null && jedis.isConnected()){
			return jedis;
		}else{
			if(pool != null){
				jedis = pool.getResource();
				return jedis;
			}else{
				pool = getPool();
				jedis = pool.getResource();
				return jedis;
			}
		}
	}
	
	public String getColume(String security, String name){
		return security + ":" + name;
	}
	
	public List<Object> getLatest(String security, String[] columns){
		HashMap<String, Object> candle = new HashMap<>();
		Pipeline pipeline = jedis.pipelined();
		pipeline.multi();
		for (String column : columns) {
			pipeline.lindex(getColume(security, column), -1);
		}
		pipeline.exec();
		List<Object> result = pipeline.syncAndReturnAll();
		return (List<Object>) result.get(result.size()-1);
	}
	
	public void insertCandle(String security, Date time, String open, String high, String low, String close, String volume){
		Pipeline pipeline = jedis.pipelined();
		pipeline.multi();
		pipeline.rpush(getColume(security, "datetime"), time.toLocaleString());
		pipeline.rpush(getColume(security, "open"), open);
		pipeline.rpush(getColume(security, "high"), high);
		pipeline.rpush(getColume(security, "low"), low);
		pipeline.rpush(getColume(security, "close"), close);
		pipeline.rpush(getColume(security, "volume"), volume);
		pipeline.exec();
		pipeline.sync();
	}
	
	public void updateCandle(String security, String open, String high, String low, String close, String volume, long index){
		Pipeline pipeline = jedis.pipelined();
		pipeline.multi();
		pipeline.lset(getColume(security, "open"), index, open);
		pipeline.lset(getColume(security, "high"), index, high);
		pipeline.lset(getColume(security, "low"), index, low);
		pipeline.lset(getColume(security, "close"), index, close);
		pipeline.lset(getColume(security, "volume"), index, volume);
		pipeline.exec();
		pipeline.sync();
	}
	
	private String strPrice(int price){
		return String.valueOf(price/10000.0);
	}
	
	public void setPriceLevel(String security, Date time, JSONArray asks, JSONArray bids){
		Pipeline pipeline = jedis.pipelined();
		pipeline.multi();
		String Key = getColume(security, "ask");
		pipeline.del(Key);
		for (Object ask : asks) {
			pipeline.hset(Key, ((JSONArray)ask).get(0).toString(), ((JSONArray)ask).get(1).toString());
		}
		
		Key = getColume(security, "bid");
		pipeline.del(Key);
		for (Object bid : bids) {
			pipeline.hset(Key, ((JSONArray)bid).get(0).toString(), ((JSONArray)bid).get(1).toString());
		}
		
		pipeline.set(getColume(security, "latest"), time.toLocaleString());
		pipeline.exec();
		pipeline.sync();
	}
	
	public void expire(String security, String[] columns, int seconds){
		Pipeline pipeline = jedis.pipelined();
		pipeline.multi();
		for (String column : columns) {
			pipeline.expire(getColume(security, column), seconds);
		}
		pipeline.exec();
		pipeline.sync();
	}
	
	public void expireAt(String security, String[] columns, long unixTime){
		Pipeline pipeline = jedis.pipelined();
		pipeline.multi();
		for (String column : columns) {
			pipeline.expireAt(getColume(security, column), unixTime);
		}
		pipeline.exec();
		pipeline.sync();
	}

	
	public boolean hasTable(String security, String[] columns){
		Pipeline pipeline = jedis.pipelined();
		pipeline.multi();
		for (String column : columns) {
			pipeline.exists(getColume(security, column));
		}
		pipeline.exec();
		List<Object> list = pipeline.syncAndReturnAll();
		for (boolean boo : (List<Boolean>) list.get(list.size()-1)) {
			if(!boo){
				return false;
			}
		} 
		return true;
		
	}
	
}
