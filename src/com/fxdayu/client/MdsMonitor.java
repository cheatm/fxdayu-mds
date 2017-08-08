package com.fxdayu.client;

import java.awt.print.Printable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.quant360.api.callback.MdsCallBack;
import com.quant360.api.client.Client.ClientStatus;
import com.quant360.api.client.MdsClient;
import com.quant360.api.client.impl.MdsClientImpl;
import com.quant360.api.model.mds.MdsIndexSnapshotBody;
import com.quant360.api.model.mds.MdsLogonReq;
import com.quant360.api.model.mds.MdsMktDataRequestEntry;
import com.quant360.api.model.mds.MdsMktDataRequestReq;
import com.quant360.api.model.mds.MdsMktDataSnapshotHead;
import com.quant360.api.model.mds.MdsPriceLevel;
import com.quant360.api.model.mds.MdsReq;
import com.quant360.api.model.mds.MdsStockSnapshotBody;
import com.quant360.api.model.mds.enu.MdsExchangeId;
import com.quant360.api.model.mds.enu.MdsMktSubscribeFlag;
import com.quant360.api.model.mds.enu.MdsPriceLevelOperator;
import com.quant360.api.model.mds.enu.MdsSecurityType;
import com.quant360.api.model.mds.enu.MdsSubscribeMode;
import com.quant360.api.model.mds.enu.MdsSubscribedTickType;
import com.quant360.api.model.oes.enu.ErrorCode;

import com.fxdayu.client.Config;
import com.fxdayu.client.IO.MQManager;
import com.fxdayu.client.IO.RedisHandler;
import com.fxdayu.client.IO.SaveManager;
import com.fxdayu.client.enums.PriceTick;

public class MdsMonitor extends TimeMonitor{
	
	private MdsClient client;
	private Config config;
	private Logger logger;
	private MdsCallBack callBack;
	private RedisHandler handler;
	private SaveManager manager;
	private MQManager mq;
	private Date BEGIN;
	private Date END;
	
	public static void main(String[] args) throws InterruptedException {
		// TODO Auto-generated method stub
		MdsMonitor monitor = new MdsMonitor(args);
		monitor.start();
		monitor.join();
	}
	
	
	public MdsMonitor(String[] args){
		super();
		if (args.length==1) {
			config = new Config(args[0]);
		}else{
			config = new Config();
		}
		
		initTimeRange();
		initModels();
		initClient();
	}
	
	private void initTimeRange(){
		Date now = new Date();
		
		BEGIN = (Date) now.clone();
		END = (Date) now.clone();
		
		BEGIN.setHours(9);
		END.setHours(15);
		END.setMinutes(15);
		
	}
	
	private void initModels(){
		logger = config.logger;
//		handler = new RedisHandler(config.getRedisConfig());
		callBack = new MdsServiceCallBack();
		manager = new SaveManager(config);
		try {
			mq = new MQManager(config.getMqConfig());
		} catch (IOException | TimeoutException e) {
			// TODO Auto-generated catch block
			logger.error("Initialize mq failed", e);
			System.exit(0);
		}
	}
	
	private void initClient(){
		try {
			client = new MdsClientImpl(config.getApiConfigPath());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		client.initCallBack(callBack);
		
	}
	
	private String logonFailMessage(String clientName, String message, ErrorCode code){
		return String.format("MDS客户端: %s 登录失败 ! %s errorCode = %s", client, message, code);
	}
	
	public void startClient(){
		ErrorCode code = ErrorCode.SUCCESS;
		try {			
			code = client.start(buildLogonReq());				
		} catch (Exception e) {
			MdsReq logonReq = (MdsReq)client.getLogonMsg();
			String clientName = logonReq.getLogonReq().getUsername();
			logger.error(String.format("MdsClient %s logon fail", clientName), e);
			return;
		}
		MdsReq logonReq = (MdsReq)client.getLogonMsg();
		String clientName = logonReq.getLogonReq().getUsername();
	
		switch (code) {
		case SUCCESS:
			logger.info(String.format("MdsCient %s logon success", clientName));
			subscribe();
			break;
		case CLIENT_DISABLE:
			logger.error(logonFailMessage(clientName, "CLIENT_DISABLE", code));
			break;
		case INVALID_CLIENT_NAME:
			logger.error(logonFailMessage(clientName, "INVALID_CLIENT_NAME", code));
			break;
		case INVALID_CLIENT_PASSWORD:
			logger.error(logonFailMessage(clientName, "INVALID_CLIENT_PASSWORD", code));
			break;
		case CLIENT_REPEATED:
			logger.error(logonFailMessage(clientName, "CLIENT_REPEATED", code));
			break;
		case CLIENT_CONNECT_OVERMUCH:
			logger.error(logonFailMessage(clientName, "CLIENT_CONNECT_OVERMUCH", code));
			break;
		case INVALID_PROTOCOL_VERSION:
			logger.error(logonFailMessage(clientName, "INVALID_PROTOCOL_VERSION", code));
			break;
		case OTHER_ERROR:
			logger.error(logonFailMessage(clientName, "OTHER_ERROR", code));
			break;
		}
	}	
	
	public void subscribe(){
		subMktData(config.getMktDataSubscribeConfig());
	}
	
	public void subMktData(JSONObject dataSubscribeConfig){
		List<MdsMktDataRequestEntry> entries = getEntries(dataSubscribeConfig.getJSONObject("ENTRIES"));
		MdsMktDataRequestReq mktDataRequestReq = getMktDataRequestReq(dataSubscribeConfig.getJSONObject("subscribe"));
		if (entries.size() > 0) {
			mktDataRequestReq.setSubSecurityCnt(entries.size());
		}
		
		client.subscribeMarketData(mktDataRequestReq, entries);
		logger.info("Subscribe MarketData");
	}
	
	public MdsMktDataRequestReq getMktDataRequestReq(JSONObject reqConfig){
		MdsMktDataRequestReq req = new MdsMktDataRequestReq();
		
		for (String key : reqConfig.keySet()) {
			switch (key) {
			case "subMode":
				req.setSubMode(MdsSubscribeMode.valueOf(reqConfig.getInt(key)));
				break;
			case "tickType":
				req.setTickType(MdsSubscribedTickType.valueOf(reqConfig.getInt(key)));
				break;
			case "sseStockFlag":
				req.setSseStockFlag(MdsMktSubscribeFlag.valueOf(reqConfig.getInt(key)));
				break;
			case "sseIndexFlag":
				req.setSseIndexFlag(MdsMktSubscribeFlag.valueOf(reqConfig.getInt(key)));
				break;
			case "sseOptionFlag":
				req.setSseOptionFlag(MdsMktSubscribeFlag.valueOf(reqConfig.getInt(key)));
				break;
			case "szseStockFlag":
				req.setSzseStockFlag(MdsMktSubscribeFlag.valueOf(reqConfig.getInt(key)));
				break;
			case "szseIndexFlag":
				req.setSzseIndexFlag(MdsMktSubscribeFlag.valueOf(reqConfig.getInt(key)));
				break;
			case "szseOptionFlag":
				req.setSzseOptionFlag(MdsMktSubscribeFlag.valueOf(reqConfig.getInt(key)));
				break;
			case "isRequireInitialMktData":
				req.setRequireInitialMktData(reqConfig.getBoolean(key));
				break;
			case "dataTypes":
				req.setDataTypes(reqConfig.getInt(key));
				break;
			case "beginTime":
				req.setBeginTime(reqConfig.getInt(key));
				break;
			
			default:
				break;
			}
		}
		return req;
	}
	
//	向MdsMktDataRequestEntry表中添加元素
	public ArrayList<MdsMktDataRequestEntry> appendEntryList(ArrayList<MdsMktDataRequestEntry> entryList, JSONArray securityList, 
			MdsExchangeId exchId, MdsSecurityType securityType){
		for (Object securityId : securityList) {
			entryList.add(creatMdsEntry((int)securityId, exchId, securityType));
		}
		return entryList;
		
	}
	
//  生成MdsMktDataRequestEntry对象
	public MdsMktDataRequestEntry creatMdsEntry(int instrId, MdsExchangeId exchId, MdsSecurityType securityType){
		MdsMktDataRequestEntry entry = new MdsMktDataRequestEntry();
		entry.setInstrId(instrId);
		entry.setExchId(exchId);
		entry.setSecurityType(securityType);
		
		return entry;
	}
	
//	根据JSON生成MdsMktDataRequestEntry表
	public ArrayList<MdsMktDataRequestEntry> getEntries(JSONObject subscribeConfig){
		ArrayList<MdsMktDataRequestEntry> entryList = new ArrayList<MdsMktDataRequestEntry>();
		for (String exchId : subscribeConfig.keySet()) {
			switch (exchId) {
			case "MDS_EXCH_SSE":
				entryList = appendEntryList(entryList, subscribeConfig.getJSONArray(exchId), 
						MdsExchangeId.MDS_EXCH_SSE, MdsSecurityType.MDS_SECURITY_TYPE_STOCK);
				break;
			case "MDS_EXCH_SZSE":
				entryList = appendEntryList(entryList, subscribeConfig.getJSONArray(exchId), 
						MdsExchangeId.MDS_EXCH_SZSE, MdsSecurityType.MDS_SECURITY_TYPE_STOCK);
				break;
			
			default:
				break;
			}
		}
		return entryList;
	}
	
	private MdsReq buildLogonReq(){
		MdsLogonReq logonReq = new MdsLogonReq();
		JSONObject clientConfig = config.getMDSClient();
		logonReq.setUsername(clientConfig.getString("username"));
		logonReq.setPassword(clientConfig.getString("password"));

		MdsReq mdsReq = new MdsReq();
		mdsReq.setLogonReq(logonReq);
		return mdsReq;
	}
	
	@Override
	protected boolean runnable() {
		Date now = new Date();
		
		if (now.after(BEGIN) && now.before(END)) return true;
		else return false;
		
	}
	
	@Override
	public void check() {
		
		switch (client.getStatus()) {
		case Stop:
			logger.info("MdsClientStatus: Stop");
			startClient();
			break;
		case Start:
			logger.info("MdsClientStatus: Start");
			break;
		case Error:
			logger.error("MdsClientStatus: Error");
			runEnd();
			break;
		case Close:	
			logger.warn("MdsClientStatus: Close");
			startClient();
			break;
		default:
			break;
		}
	}
	
	@Override
	public void runEnd(){
		client.stop();
		client.close();
		logger.info("Exit Service");
		System.exit(0);
	}
	
	class MdsServiceCallBack extends MdsCallBack{
		
		private static final int OPEN = 93000000;
		private static final int PAUSE = 113000000;
		private static final int REOPEN = 130000000;
		private static final int END = 150000000;
		
		@Override
		public void onDisConn(MdsClient client) {
			// TODO Auto-generated method stub
			
			startClient();
		}
		
		@Override
		public void onMktStock(MdsMktDataSnapshotHead head, MdsStockSnapshotBody data) {
			
			if (timeFilter(head.getUpdateTime())) {
				mq.sendExchange(data.getSecurityID(), head.getTradeDate(), head.getUpdateTime(), getTick(data));

				manager.onQuote(
						data.getSecurityID(), 
						CandleInstance.combine(head.getTradeDate(), head.getUpdateTime()), 
						data.getTradePx(), 
						data.getTotalVolumeTraded());
			}
			
		}
		
		private boolean timeFilter(int time){
			if (time>OPEN && time<=PAUSE) {
				return true;
			}else if (time>REOPEN && time<=END) {
				return true;
			}else{
				return false;
			}
		}
		
		public JSONObject getTick(MdsStockSnapshotBody data){
			JSONObject tick = new JSONObject();
			tick.put(PriceTick.ASK.value, jsonPriceLevels(data.getOfferLevels()));
			tick.put(PriceTick.BID.value, jsonPriceLevels(data.getBidLevels()));
			return tick;
		}
		
		public JSONObject jsonPriceLevels(List<MdsPriceLevel> priceLevels){
			JSONObject levels = new JSONObject();
			JSONArray prices = new JSONArray();
			JSONArray volumes = new JSONArray();
			
			for (MdsPriceLevel mdsPriceLevel : priceLevels) {
				prices.put(mdsPriceLevel.getPrice()/10000.0);
				volumes.put(mdsPriceLevel.getOrderQty());
			}
			levels.put(PriceTick.PRICE.value, prices);
			levels.put(PriceTick.VOLUME.value, volumes);
			return levels;
		}
	}
	
	
}
