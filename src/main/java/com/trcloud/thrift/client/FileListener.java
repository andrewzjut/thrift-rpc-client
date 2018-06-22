package com.trcloud.thrift.client;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.io.monitor.FileAlterationListener;
import org.apache.commons.io.monitor.FileAlterationObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.trcloud.thrift.util.Config;
import com.trcloud.thrift.util.DateUtil;
import com.trcloud.thrift.util.FileUtils;



public class FileListener implements FileAlterationListener{  
	private static final Logger logger = LoggerFactory.getLogger(FileListener.class);
	private static String messageDir = Config.messageDir;	
	private static int retentionDays = Config.retentionDays;	
	private static String suffix = Config.suffix;
	private static String dateFormat = Config.dateFormat;
	 private static ExecutorService pool = Executors.newFixedThreadPool(Config.distributeNum); 
	  
	  static{
		  File root = new File(messageDir);
			 File[] fs = root.listFiles();
			  String prefix = DateUtil.getNowdate();
			  for(int i=0; i<fs.length; i++){
				  		File file = fs[i];	
						String name = file.getName();
						if(suffix.equals(FileUtils.getSuffix(name))&&!name.startsWith(prefix)){
//						if(suffix.equals(FileUtils.getSuffix(name))){
							if(!FileUtils.isReadOff(file)){
								 pool.execute( new Thread(new FileCreateWatcher(file.getAbsolutePath())));
							}
						 }
			  }
			  
			  
			  for(int i=0; i<fs.length; i++){
				  File file = fs[i];	
				  String name = file.getName();
				  if(suffix.equals(FileUtils.getSuffix(name))){
					
						if(name.startsWith(prefix)){
							  pool.execute( new Thread(new FileCreateWatcher(file.getAbsolutePath())));
						}
				  }
			  }
	  }
    
	@Override
	public void onDirectoryChange(File arg0) {
		// TODO Auto-generated method stub
	}
	
	@Override
	public void onDirectoryCreate(File arg0) {
		// TODO Auto-generated method stub
	}

	@Override
	public void onDirectoryDelete(File arg0) {
		// TODO Auto-generated method stub
	}

	@Override
	public void onFileChange(File fileName) {

	}

	@Override
	public void onFileCreate(File fileName) {
		//monitor file create
		String name = fileName.getName();
		if(suffix.equals(FileUtils.getSuffix(name))){
				try {		//delete file
					String prefix = DateUtil.getDate(retentionDays, dateFormat);
					 File root = new File(messageDir);
					 File[] fs = root.listFiles();
					 for(int i=0; i<fs.length; i++){
						  File file = fs[i];	
						  String delName = file.getName();
									if(delName.startsWith(prefix)){
										file.delete();
										logger.info("delete read off file:"+delName);
									}
					 }
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			  pool.execute( new Thread(new FileCreateWatcher(fileName.getAbsolutePath())));
		}
	}

	@Override
	public void onFileDelete(File arg0) {
		// TODO Auto-generated method stub
	}

	@Override
	public void onStart(FileAlterationObserver arg0) {
		// TODO Auto-generated method stub
	}

	@Override
	public void onStop(FileAlterationObserver arg0) {
		// TODO Auto-generated method stub
	}  
   
  
}  