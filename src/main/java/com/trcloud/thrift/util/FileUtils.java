package com.trcloud.thrift.util;

import java.io.File;
import java.io.RandomAccessFile;


public class FileUtils {
	
	
	public static String getSuffix(String fileName){
		String suffix = "";
		try {
			suffix=fileName.substring(fileName.lastIndexOf("."));
		} catch (Exception e) {
			
			return "";
		}
		return suffix;
	}
	public static boolean isReadOff(File file) {
		long length;
		long position;
		try {
			length = file.length();
			position = 0;
			if (file.exists() && file.length() > 0) {
				File f = new File(file.getAbsolutePath()+".offset");
				RandomAccessFile ff = new RandomAccessFile(f, "rw");
				String tmp;
				while ((tmp = ff.readLine()) != null) {
					position = Long.parseLong(tmp);
				}
				ff.close();
			 }
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return length==position;
	}
	
	
	
	public static long getOffset(String offsetFileName) {
		long position = 0;
			try {
				File offsetFile = new File(offsetFileName);
				if (offsetFile.exists() && offsetFile.length() > 0) {
					RandomAccessFile ff = new RandomAccessFile(offsetFile, "rw");
					String tmp;
					while ((tmp = ff.readLine()) != null) {
						position = Long.parseLong(tmp);
					}
					ff.close();
				 }
			} catch (Exception e) {
				e.printStackTrace();
			}
		 return position;
	}


	
}
