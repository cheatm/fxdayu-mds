package com.fxdayu.client;

import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.function.LongFunction;

import com.fxdayu.client.enums.WriteQuote;

public class CandleInstance {
	int high;
	int low;
	int open;
	int close;
	long volume;
	long lastVolume;
	Date startTime;
	private Date endTime;
	Date lastTime;
	public String code;

	public CandleInstance(Date datetime, int price, long volume){
		this.open = price;
		this.low = price;
		this.high = price;
		this.close = price;
		this.volume = volume;
		this.lastVolume = volume;
		setTime(datetime);
	}
	
	public CandleInstance(String code, Date datetime, int price, long volume){
		this.open = price;
		this.low = price;
		this.high = price;
		this.close = price;
		this.volume = volume;
		this.lastVolume = volume;
		setTime(datetime);
		setCode(code);
	}
	
	public CandleInstance(Date datetime, float fPrice, long volume){
		int price = (int) fPrice*10000;
		this.open = price;
		this.low = price;
		this.high = price;
		this.close = price;
		this.volume = volume;
		setTime(datetime);
	}
	
	public CandleInstance(String code, Date datetime, float fPrice, long volume){
		int price = (int) fPrice*10000;
		this.open = price;
		this.low = price;
		this.high = price;
		this.close = price;
		this.volume = volume;
		setTime(datetime);
		setCode(code);
	}
	
	public CandleInstance(Date datetime, float open, float high, float low, float close, long volume) {
		this.open = (int) open * 10000;
		this.high = (int) high * 10000;
		this.low = (int) low * 10000;
		this.close = (int) close * 10000;
		this.volume = volume;
		this.lastVolume = volume;
		setTime(datetime);
	}
	
	public CandleInstance(String code, Date datetime, float open, float high, float low, float close, List<String> volumes) {
		this.open = (int) (open * 10000);
		this.high = (int) (high * 10000);
		this.low = (int) (low * 10000);
		this.close = (int) (close * 10000);
		volumeFromList(volumes);
		setTime(datetime);
		setCode(code);
	}
	
	private void volumeFromList(List<String> volumes){
		String vol = volumes.remove(volumes.size()-1);
		this.lastVolume = sumLongFromString(volumes);
		this.volume = this.lastVolume + Long.valueOf(vol);
		
	}
	
	public Long sumLongFromString(List<String> list){
		long sum = 0;
		for (String string : list) {
			sum += Long.valueOf(string);
		}
		return sum;
	}
	
	public void setCode(String code){
		this.code = utils.unfold(code);
	}
	
//	public static String unfold(String code){
//		if (code.length()==6) {
//			if (code.startsWith("6")) {
//				return code + ".XSHG";
//			}else if(code.startsWith("0") || code.startsWith("3")){
//				return code + ".XSHE";
//			}
//			
//		}
//		return String.valueOf(code);
//	}
//	
//	public static String fold(String code){
//		if (code.length()==11) {
//			return code.substring(0, 6);
//		}else{
//			return code;
//		}
//	}
	
	@SuppressWarnings("deprecation")
	private Date minEnd(Date time){
		if(time.getSeconds() != 0){
			Date end = (Date) time.clone();
			end.setSeconds(0);
			end.setMinutes(end.getMinutes()+1);
			return end;
		}else{
			return time;
		}
	}
	
	@SuppressWarnings("unused")
	private Date minStart(Date endTime) {
		return new Date(endTime.getTime()-60000);
	}
	
	private void setTime(Date time){
		this.lastTime = (Date) time.clone();
		this.setEndTime(minEnd(time));
		this.startTime = minStart(this.getEndTime());
	}
	
	public String strHigh(){
		return String.valueOf(high/10000.0);
	}
	
	public String strLow(){
		return String.valueOf(low/10000.0);
	}

	public String strOpen(){
		return String.valueOf(open/10000.0);
	}

	public String strClose(){
		return String.valueOf(close/10000.0);
	}

	public String strVolume(){
		return String.valueOf(this.volume-this.lastVolume);
	}

	@SuppressWarnings("deprecation")
	public static Date combine(int date, int time) {
		int day = date % 100;
		int month = (date - day)%10000 / 100;
		int year = (date)/10000;
		
		int microSecond = time % 1000;
		time = time - microSecond;
		int second = time % 100000;
		time = time - second;
		int minute = time%10000000/100000;
		int hour = time/10000000;
		return new Date(year-1900, month-1, day, hour, minute, second/1000);
	}
	
//	public int onQuote(Date time, float price, long volume){
//		int price1 = (int) price * 10000;
//		if (!this.endTime.before(time)){
//			if (price1 > high) {
//				high = price1;
//			}else if (price1 < low) {
//				low = price1;
//			}
//			close = price1;
//			this.volume = volume;
//			
//			return 0;
//			
//		}else{
//			setTime(time);
//			high = price1;
//			low = price1;
//			open = price1;
//			close = price1;
//			this.lastVolume = this.volume;
//			this.volume = volume;
//			return 1;
//		}
//		
//	}
	
	@SuppressWarnings("deprecation")
	public WriteQuote onQuote(Date time, int price, long volume){
		if(this.startTime.before(time)){
			if (!this.getEndTime().before(time)){
				if (price > high) {
					high = price;
				}else if (price < low) {
					low = price;
				}
				close = price;
				this.volume = volume;
				
				return WriteQuote.UPDATE;
				
			}else{
				setTime(time);
				high = price;
				low = price;
				open = price;
				close = price;
				this.lastVolume = this.volume;
				this.volume = volume;
				return WriteQuote.INSERT;
			}
		}else{
			return WriteQuote.ERROR;
		}
	}
	
	@Override
	public String toString() {
		return String.format(
				"Candle(code=%s, start=%s, end=%s, open=%d, high=%d, low=%d, close=%d, volume=%d)", 
				code, startTime, getEndTime(), open, high, low, close, volume-lastVolume
				);
	}

	public Date getEndTime() {
		return endTime;
	}

	public void setEndTime(Date endTime) {
		this.endTime = endTime;
	}
	
}
