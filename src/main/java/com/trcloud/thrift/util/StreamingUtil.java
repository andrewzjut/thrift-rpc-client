package com.trcloud.thrift.util;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.codec.binary.Base64;

import java.io.ByteArrayOutputStream;

public class StreamingUtil {
	
	
	public static <T> byte [] obj2AvroBytes(T message) throws Exception{
		Schema schema = ((GenericContainer) message).getSchema();
		   ByteArrayOutputStream out=new ByteArrayOutputStream();  
		 DatumWriter<T> writer = new SpecificDatumWriter<T>(schema);
	        Encoder encoder= EncoderFactory.get().binaryEncoder(out,null);  
	        writer.write(message, encoder);  
	        encoder.flush();  
	        out.close();  
	      return out.toByteArray();
	}
	public static String byte2String(byte [] b){
		return  new String(Base64.encodeBase64(b));
	}
	public static byte[] String2Byte(String s){
		return Base64.decodeBase64(s);
	}
	

}
