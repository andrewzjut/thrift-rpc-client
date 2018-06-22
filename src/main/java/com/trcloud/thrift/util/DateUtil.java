package com.trcloud.thrift.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {
	
	public static String getNowdate(String pattern){
		 DateFormat fmtDateTime = new SimpleDateFormat(pattern);
		 return  fmtDateTime.format(new Date());
	}
	public static String getNowdate(){
		DateFormat fmtDateTime = new SimpleDateFormat(Config.dateFormat);
		 return  fmtDateTime.format(new Date());
	}
	
	
	public static String getDate(int beforeDay,String pattern){
		SimpleDateFormat format = new SimpleDateFormat(pattern);
		Calendar calendar = Calendar.getInstance();
		calendar.add(Calendar.DAY_OF_WEEK, -beforeDay);
		return format.format(calendar.getTime());
	}


}
