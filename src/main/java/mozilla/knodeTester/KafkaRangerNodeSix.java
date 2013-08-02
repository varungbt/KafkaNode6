package mozilla.knodeTester;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import kafka.api.FetchRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import kafka.utils.Utils;


import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.mozilla.bagheera.BagheeraProto.BagheeraMessage;

public class KafkaRangerNodeSix {

/**
* @param args
* @throws IOException
* @throws NumberFormatException
* @throws ParseException
*/
public static void main(String[] args) throws NumberFormatException, IOException, ParseException {
// TODO Auto-generated method stub

		SimpleConsumer consumer = new SimpleConsumer("127.0.0.1", 9092, 10000,
				Integer.MAX_VALUE);
		long[] offLst1 = consumer.getOffsetsBefore("metrics", 1, -1,
				Integer.MAX_VALUE);

		List<Long> offLst = new ArrayList<Long>();
		for (long off : offLst1)
			offLst.add(off);

		System.out
				.println("How many days do you want to go back in time ? !!!");
		InputStreamReader ir = new InputStreamReader(System.in);

		BufferedReader br = new BufferedReader(ir);
		int noDays = Integer.parseInt(br.readLine());

		System.out
				.println("## Attempting to get all the messages that match the filter condition ##");

		// 0 being partial message neglect it
		long offset = offLst.get(1);
		long markedOffst = 0;
		long currentOffst = 0;

		long offstIndex = 0;

		/* checking the sparse list of offsets for fixing the go-back point */
		for (int i = 1; i < offLst.size(); i++) {
			FetchRequest fr = new FetchRequest("metrics", 1, offLst.get(i),
					Integer.MAX_VALUE);
			ByteBufferMessageSet message = consumer.fetch(fr);
			Date d1 = new Date();

			for (MessageAndOffset msg : message) {
				
				BagheeraMessage bmsg=BagheeraMessage.parseFrom(ByteString.copyFrom(msg.message().payload()));

				
				String dateTemp = new java.text.SimpleDateFormat("yyyy/MM/dd HH:mm:ss").format(new java.util.Date (bmsg.getTimestamp()));
				Date d2 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss").parse(dateTemp);
				
				long diffVal = d1.getTime() - d2.getTime();
				
				long MILLIS_PER_DAY = 24 * 3600 * 1000;
				long dayDiff = Math.round(diffVal / ((double) MILLIS_PER_DAY));

				if (!(dayDiff <= noDays)) {
					/* I want to go back like three days but currently i am two days back - so consider the 2nd back day
					 * if not the number of days are greater than the time you
					 * want to go back
					 */
					/* =>stop */
					markedOffst = offLst.get(i);
					currentOffst = offLst.get(i);
					offstIndex = i;

					break;
				} else {
					
					//I want to go back like three days but currently i am two days back - so consider the 2nd back day
					// offset=msg.offset();
					currentOffst = offLst.get(i);
					offstIndex = i;
				}

			}

			if (markedOffst > 0)
				break;

		}
		long checkpoint = offstIndex;
		/* Start checking messages from this checkpoint */

		//File f = new File("/home/vmanohar/Desktop/b.out");

		//if (!f.exists())
			//f.createNewFile();

		//FileWriter fw = new FileWriter(f.getAbsolutePath(), true);
		//BufferedWriter bw = new BufferedWriter(fw);

		//bw.write("### RECORDS MATCHING USER FILTER CONDITIONS ARE ###");
		long offsetFinal = offLst.get((int) (checkpoint));
		long endPoint = offLst.get(1);
		/* offLst.get(1) will have the highest value of the offset */
		
		long totalCount=0;
		while (offsetFinal <= offLst.get(1)) {
			// fetch the messages starting from offsetFinal
			FetchRequest fetchRequest = new FetchRequest("metrics", 1,
					offsetFinal, Integer.MAX_VALUE);
			ByteBufferMessageSet messages = consumer.fetch(fetchRequest);

			for (MessageAndOffset msg : messages) {
			
				
				
				totalCount++;
				offsetFinal = msg.offset();
				//bw.write(Utils.toString(msg.message().payload(), "UTF-8"));
				//bw.write("\n");
			}
		}
		//bw.close();
		//fw.close();
		//System.out.println("RECORDS FETCHING COMPLETE");
		System.out.println("total number of messages are "+ totalCount);
	}

}

