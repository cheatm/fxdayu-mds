package com.fxdayu.client;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Paths;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.RollingFileAppender;
import org.json.JSONObject;
import org.json.JSONArray;

import com.google.common.base.Predicate;
import com.quant360.api.utils.StringUtils;


public class Config {
	
	public static final String ENVIRONMENT = "MDSCLIENT";
	
	private static String apiConfigPath = "quant_api_config.json";
	private static String clientInfoPath = "clientInfo.json";
	private static String mktDataSubscribePath = "mktDataSubscribe.json";
	private static String redisConfigPath = "redis_config.json";
	private static String logConfigPath = "LogProperty.conf";
	private static String mqConfigPath = "MQConfig.json";
	private static String rootdir;
	private JSONObject clientInfo;
	private JSONObject mktDataSubscribeConfig;
	private JSONObject redisConfig;
	private JSONObject mqConfig;
	public Logger logger;
	
	public static void main(String[] args) {
		
		Config config;
		if (args.length==1) {
			config = new Config(args[0]);
		}else{
			config = new Config();
		}
	}
	
	public String absPath(String relativePath){
		return Paths.get(this.rootdir, relativePath).toString();
	}
	
	private void init(String root){
		this.rootdir = root;
		this.apiConfigPath = absPath(this.apiConfigPath);
		this.clientInfoPath = absPath(this.clientInfoPath);
		this.mktDataSubscribePath = absPath(this.mktDataSubscribePath);
		this.redisConfigPath = absPath(this.redisConfigPath);
		this.logConfigPath = absPath(this.logConfigPath);
		this.mqConfigPath = absPath(this.mqConfigPath);
		
		try {
			this.logger = createLoger();
			this.clientInfo = loadJSON(this.clientInfoPath);
			this.mktDataSubscribeConfig = loadJSON(this.mktDataSubscribePath);
			this.redisConfig = loadJSON(this.redisConfigPath);
			this.mqConfig = loadJSON(this.mqConfigPath);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.exit(0);
		}
	}
	
	public Config(){
		if (System.getenv().containsKey(ENVIRONMENT)) {
			init(System.getenv(ENVIRONMENT));
		}else{
			init(System.getProperty("user.dir")+"/config");
		}
	}
	
	public Config(String root){
		init(root);
	}
	
	public Logger createLoger(){
		Logger logger = Logger.getRootLogger();
		PropertyConfigurator.configure(this.logConfigPath);
		return logger;
	}
	
	public String getApiConfigPath() {
		return apiConfigPath;
	}
	
	public JSONObject getClientInfo(){
		return this.clientInfo;
	}
	
	public JSONObject getMDSClient(){
		return this.clientInfo.getJSONObject("MDSCLIENT");
	}
	
	public JSONArray getOESClient(){
		return this.clientInfo.getJSONArray("OESCLIENT");
	}
	
	public JSONObject loadJSON(String confPath) throws Exception
	  {
	    StringBuilder builder = new StringBuilder();
	    String line;
	    
	      File file = new File(confPath);
	      if (!file.isFile()) {
	        throw new FileNotFoundException();
	      }
	      List<String> lines = FileUtils.readLines(file, StringUtils.UTF8_CHARSET);
	      for (String li : lines)
	      {
	        if (!li.trim().startsWith("//")) {
	          builder.append(li.trim());
	        }
	      }
	    
	    return new JSONObject(builder.toString());
	  }
	
	public JSONObject getMktDataSubscribeConfig(){
		return this.mktDataSubscribeConfig;
	}
	
	public JSONObject getRedisConfig(){
		return this.redisConfig;
	}
	
	public JSONObject getMqConfig(){
		return this.mqConfig;
	}
	
}
