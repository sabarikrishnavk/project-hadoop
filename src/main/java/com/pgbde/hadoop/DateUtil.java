package com.pgbde.hadoop;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.time.DateUtils;

public class DateUtil {

	public static final String[] FORMAT = new String[] { "yyyy-MM-dd" };
	public static DateFormat dateFormat = new SimpleDateFormat(FORMAT[0]);

	public static void main(String[] args) {
		System.out.println(DateUtil.getNextDateString("2017-12-14"));
		System.out.println(DateUtil.getDay("2017-12-14"));
	}

	/**
	 * Util method to get next date in the correct format
	 * 
	 * @param dateString
	 * @return
	 */
	public static String getNextDateString(String inputDate) {
		try {
			Date nextDay = DateUtils.addDays(
					DateUtils.parseDate(inputDate, FORMAT), 1);
			String strDate = dateFormat.format(nextDay);
			return strDate;
		} catch (Exception e) {
			System.out.println("DateUtil error : " + inputDate);
		}
		return inputDate;
	}

	/**
	 * Util method to get Day of month
	 */

	public static int getDay(String inputDate) {
		try {
			return  Integer.parseInt(inputDate.split("-")[2]);

		} catch (Exception e) {
			System.out.println("DateUtil : getDate: error : " + inputDate);
			return 0;
		}
	}

}
