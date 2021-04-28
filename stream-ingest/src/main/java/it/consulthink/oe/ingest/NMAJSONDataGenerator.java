/**
 * 
 */
package it.consulthink.oe.ingest;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Stream;

import it.consulthink.oe.model.NMAJSONData;

/**
 * @author svetr
 *
 */
public class NMAJSONDataGenerator {
	
	private static final Random r = new Random(System.currentTimeMillis());
	
	public static final HashMap<String, Long> ips = new HashMap<String, Long>();
	public static final HashMap<String, Long> ipsLocal = new HashMap<String, Long>();
	
	private static String generateWellKnownPort() {
		boolean isWebPort = r.nextInt(3) == 0;
		String[] webPort = {"443","80"};
		if (isWebPort) {
			return webPort[r.nextInt(2)];
		}
				
		return String.valueOf(r.nextInt(49152));
	}
	
	private static String generateDinamicPort() {
		return String.valueOf(49151 + r.nextInt(16383));
	}
	
	private static long generateBytesIn() {
		return r.nextInt(84596);
	}
	
	private static long generateBytesOut() {
		return r.nextInt(84596);
	}
	
	private static long generatePacketsIn() {
		return r.nextInt(216);
	}	
	
	private static long generatePacketsOut() {
		return r.nextInt(216);
	}	
	
	private static String generateRandomLocalIP() {
		String result = null; 
		if (ipsLocal.size() == 0 || r.nextInt(50) == 0) {
			result = 10 + "." + 10 + "." + 10 + "." + r.nextInt(256);
			if (ips.containsKey(result)) {
				ipsLocal.put(result, ipsLocal.get(result) +1l);
			}else {
				ipsLocal.put(result, 1l);
			}
		}else {
			Iterator<String> iterator = ipsLocal.keySet().iterator();
			Long max = Collections.max(ipsLocal.values());
			int v = r.nextInt(ipsLocal.size());
			for (int i = 0; iterator.hasNext(); i++) {
				result = iterator.next();
				if (i >= v && ipsLocal.get(result) < max)
					break;
			}
			ipsLocal.put(result, ipsLocal.get(result) +1l);
		}
		
		return result;
	}	
	
	private static String generateRandomIP() {
		String result = null; 
		if (ips.size() == 0 || r.nextInt(3) == 0) {
			result = r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256) + "." + r.nextInt(256);
			if (ips.containsKey(result)) {
				ips.put(result, ips.get(result) +1l);
			}else {
				ips.put(result, 1l);
			}
		}else {
			Iterator<String> iterator = ips.keySet().iterator();
			Long max = Collections.max(ips.values());
			int v = r.nextInt(ips.size());
			for (int i = 0; iterator.hasNext(); i++) {
				result = iterator.next();
				if (i >= v && ips.get(result) < max)
					break;
			}
			ips.put(result, ips.get(result) +1l);
		}
		
		return result;
	}
	
	public static Stream<NMAJSONData> generateInfiniteStream(List<String> myIps){
		
		return Stream.generate(new Supplier<NMAJSONData>() {

			@Override
			public NMAJSONData get() {
//				try {
//					Thread.sleep(1 + r.nextInt(1000));
//				} catch (Throwable e) {
//					e.printStackTrace();
//				}
				return r.nextInt(100) == 0 ? generateAnomaly(myIps) : generateStandard(myIps);
			}
			
			
		
			
		});
	}
	
		
	public static NMAJSONData generateAnomaly(List<String> myIps) {
		
		NMAJSONData result = generateStandard(myIps);
		result.setBytesin(result.getBytesin() * (1+ r.nextInt(2)));
		result.setBytesout(result.getBytesout()* (1+ r.nextInt(2)));
		
		result.setPktsin(result.getPktsin() * (1+ r.nextInt(2)));
		result.setPktsout(result.getPktsout() * (1+ r.nextInt(2)));
		result.setPkts(result.getPktsin() + result.getPktsout());
		
		result.setPost((result.getPost() + 1l) * r.nextInt(2));
		result.setGet((result.getGet() + 1l) * r.nextInt(2));
		
		return result;
		
		
	}
	
	public static NMAJSONData generateStandard(List<String> myIps) {
		if (myIps == null || myIps.isEmpty()) {
			String str = ""
	                + "213.61.202.114,"
	                + "213.61.202.115,"
	                + "213.61.202.116," 
	                + "213.61.202.117,"
	                + "213.61.202.118,"
	                + "213.61.202.119," 
	                + "213.61.202.120,"
	                + "213.61.202.121,"
	                + "213.61.202.122," 
	                + "213.61.202.123,"
	                + "213.61.202.124,"
	                + "213.61.202.125,"
	                + "213.61.202.126";
			String[] a = str.split(",");
			myIps = Arrays.asList(a);			
		}

		
		NMAJSONData result = null;
		
		Date time = new Date(System.currentTimeMillis());
		
		
		boolean lateral = r.nextInt(100) == 0;
		
		String src_ip;
		String dst_ip;
		if (lateral) {
			src_ip = generateRandomLocalIP();
			dst_ip = generateRandomLocalIP();
		}else {
			boolean isSrcIpMyIp = r.nextBoolean();
			src_ip = isSrcIpMyIp ? myIps.get(r.nextInt(myIps.size())) : generateRandomIP();
			dst_ip = !isSrcIpMyIp ? myIps.get(r.nextInt(myIps.size())) : generateRandomIP();
			
		}
		
		boolean isDportWellKnown = r.nextBoolean();
		String dport = isDportWellKnown ? generateWellKnownPort() : generateDinamicPort();
		String sport = !isDportWellKnown ? generateWellKnownPort(): generateDinamicPort();
		long bytesin = generateBytesIn();
		long bytesout = generateBytesOut();
		
		long pktsin = generatePacketsIn();
		long pktsout = generatePacketsOut();
		
		long pkts = pktsin + pktsout;
		
		long synin = r.nextInt(1 + (int)pktsin);
		long synackout = r.nextInt(1 + (int)pktsout);
		long rstin = pktsin - synin;
		long rstout = pktsout - synackout;
		
//		TODO
		long fin = r.nextInt(1000) == 0 ? 1l : 0l;
		
		long get = 0l;
		long post = 0l;
		
		
		if (dport.equals("80") || dport.equals("443") || sport.equals("80") || sport.equals("443")) {
			boolean isPost = r.nextInt(20) == 0;	

			get = r.nextInt(1 + (int)Math.floorDiv(pkts, 20));
			post = isPost ? r.nextInt(1+(int)get) : 0;			
		}

		
		result = new NMAJSONData(time, src_ip, dst_ip, dport, sport, bytesin, bytesout, pkts, pktsin, pktsout, synin, synackout, rstin, rstout, fin, get, post);
		
		
		return result;
	}
	


}
