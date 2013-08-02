package mozilla.knodeTester;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.mozilla.bagheera.BagheeraProto.BagheeraMessage;
//import com.mozilla.bagheera.BagheeraProto.BagheeraMessage;


public class testingCluster {
	
	public static void main(String[] args) throws InvalidProtocolBufferException, UnsupportedEncodingException {
		
	
		SimpleConsumer consumer = new SimpleConsumer("127.0.0.1",9092,10000,Integer.MAX_VALUE);
		long[] offLst1=consumer.getOffsetsBefore("metrics", 0, -1, 100);
		
		List<Long> offLst=new ArrayList<Long>();
		for(long off:offLst1)
		{
			offLst.add(off);
			System.out.println(off);
		}
		
	
		/*fetch the messages */
		
		long startingOffset = 34317812369L;
		

		FetchRequest fr= new FetchRequest("metrics", 0, startingOffset, Integer.MAX_VALUE);
	
		kafka.javaapi.message.ByteBufferMessageSet message= consumer.fetch(fr);
		
		
		
		for(MessageAndOffset msg: message)
		{
			
			
			BagheeraMessage bmsg=BagheeraMessage.parseFrom(ByteString.copyFrom(msg.message().payload()));
			System.out.println(bmsg.getId());
			//System.out.println(new String(bmsg.getPayload().toByteArray(),"UTF-8"));
		
			System.out.println(bmsg.getTimestamp());
		
		
		}
		
		
		//	ByteBufferMessageSet message =  consumer.fetch(fr);
		
		
		
		
		
	}

}
