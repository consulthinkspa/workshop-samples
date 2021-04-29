/**
 * 
 */
package it.consulthink.oe.ingest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.function.Supplier;
import java.util.stream.Stream;

import it.consulthink.oe.model.NMAJSONData;

/**
 * @author svetr
 *
 */
public class NMAJSONDataGenerator {
	
	


	public static final int MEAN_BYTESIN = 270;
	public static final int MEAN_BYTESOUT = 398;
	public static final int MEAN_PACKETS_IN = 2;
	public static final int MEAN_PACKETS_OUT = 2;
	
	public static final int MAX_BYTESIN = 84596;
	public static final int MAX_BYTESOUT = 294358;
	public static final int MAX_BYTES = MAX_BYTESIN + MAX_BYTESOUT;
	public static final int MAX_PACKETS_IN= 216;
	public static final int MAX_PACKETS_OUT= 216;
	public static final int MAX_PACKETS= MAX_PACKETS_IN + MAX_PACKETS_OUT;
	public static final int MAX_SYNIN= 3;
	public static final int MAX_SYNACKOUT= 216;
	public static final int MAX_RSTIN= 6;
	public static final int MAX_RSTOUT= 4;

	public static final Random r = new Random(System.currentTimeMillis());
	
	public static final HashMap<String, Long> ips = new HashMap<String, Long>();
	public static final HashMap<String, Long> ipsLocal = new HashMap<String, Long>();
	
	private static String generateWellKnownPort() {
		boolean isWebPort = r.nextInt(2) == 0;
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
		long result = MEAN_BYTESIN;
		if (r.nextBoolean()) {
			int maxRandomInt = Math.floorDiv(MAX_BYTESIN - (int)MEAN_BYTESIN, 2);
			result = result + (long) r.nextInt(maxRandomInt);
		}else {
			int maxRandomInt = Math.floorDiv((int)MEAN_BYTESIN, 2);
			result = result - (long) r.nextInt(maxRandomInt);
		}
		return Math.max(Math.min(result, MAX_BYTESIN),0);
	}
	
	private static long generateBytesOut() {
		long result = MEAN_BYTESOUT;
		if (r.nextBoolean()) {
			int maxRandomInt = Math.floorDiv(MAX_BYTESOUT - (int)MEAN_BYTESOUT, 2);
			result = result + (long) r.nextInt(maxRandomInt);
		}else {
			int maxRandomInt = Math.floorDiv((int)MEAN_BYTESOUT, 2);
			result = result - (long) r.nextInt(maxRandomInt);
		}
		return Math.max(Math.min(result, MAX_BYTESOUT),0);
	}
	
	private static long generatePacketsIn() {
		long result = MEAN_PACKETS_IN;
		if (r.nextBoolean()) {
			int maxRandomInt = Math.floorDiv(MAX_PACKETS_IN - (int)MEAN_PACKETS_IN, 2);
			result = result + (long) r.nextInt(maxRandomInt);
		}else {
			int maxRandomInt = Math.floorDiv((int)MEAN_PACKETS_IN, 2);
			result = result - (long) r.nextInt(maxRandomInt);
		}
		return Math.max(Math.min(result, MAX_PACKETS_IN),0);		
	}	
	
	private static long generatePacketsOut() {
		long result = MEAN_PACKETS_OUT;
		if (r.nextBoolean()) {
			int maxRandomInt = Math.floorDiv(MAX_PACKETS_OUT - (int)MEAN_PACKETS_OUT, 2);
			result = result + (long) r.nextInt(maxRandomInt);
		}else {
			int maxRandomInt = Math.floorDiv((int)MEAN_PACKETS_OUT, 2);
			result = result - (long) r.nextInt(maxRandomInt);
		}
		return Math.max(Math.min(result, MAX_PACKETS_OUT),0);	
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
	

	public static Stream<NMAJSONData> generateInfiniteStream(Collection<String> myIps){
		
		return Stream.generate(new Supplier<NMAJSONData>() {

			@Override
			public NMAJSONData get() {
//				try {
//					Thread.sleep(1 + r.nextInt(1000));
//				} catch (Throwable e) {
//					e.printStackTrace();
//				}
				return r.nextInt(95) == 0 ? generateAnomaly(myIps) : generateStandard(myIps);
			}
			
			
		
			
		});
	}
	
	public static Stream<NMAJSONData> generateInfiniteStreamNoAnomaly(Collection<String> myIps){
		
		return Stream.generate(new Supplier<NMAJSONData>() {
			@Override
			public NMAJSONData get() {
				return generateStandard(myIps);
			}
			
		});
	}	
	
		
	public static NMAJSONData generateAnomaly(Collection<String> myIps) {
		
		NMAJSONData result = new NMAJSONDataAnomaly(generateStandard(myIps));
		
		return result;
		
		
	}
	
	public static NMAJSONData generateStandard(Collection<String> ips) {
		if (ips == null || ips.isEmpty()) {
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
			ips = Arrays.asList(a);			
		}
		List<String> myIps = new ArrayList<String>(ips);
		
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
		
		long synin = Math.min(r.nextInt(1 + (int)pktsin), MAX_SYNIN);
		long synackout = Math.min(r.nextInt(1 + (int)pktsout), MAX_SYNACKOUT);
		long rstin = Math.min(pktsin - synin, MAX_RSTIN);
		long rstout = Math.min(pktsout - synackout, MAX_RSTOUT);
		
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
	
//	public static class NMAJSONDataAnomaly extends NMAJSONData{
//		/**
//		 * 
//		 */
//		private static final long serialVersionUID = 1L;
//
//		public NMAJSONDataAnomaly(NMAJSONData input) {
//			super();
//			
//			this.time = input.getTime();
//			this.src_ip = input.getSrc_ip();
//			this.dst_ip = input.getDst_ip();
//			this.dport = input.dport;
//			this.sport = input.sport;
//			this.synin = input.synin;
//			this.synackout = input.synackout;
//			this.rstin = input.rstin;
//			this.rstout = input.rstout;
//			this.fin = input.fin;
//			
//			
//			this.setBytesin(input.getBytesin() + MAX_BYTESIN);
//			this.setBytesout(input.getBytesout() + MAX_BYTESOUT);
//			
//			this.setPktsin(input.getPktsin() + MAX_PACKETS_IN);
//			this.setPktsout(input.getPktsout() + MAX_PACKETS_OUT);
//			this.setPkts(input.getPktsin() + input.getPktsout());
//			
//			this.setPost((input.getPost() + 1l) * r.nextInt(2));
//			this.setGet((input.getGet() + 1l) * r.nextInt(2));
//		}
//	}
	
	
	public static class NMAJSONDataAnomaly extends NMAJSONData{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		
		

		public NMAJSONDataAnomaly() {
			super();
		}



		public NMAJSONDataAnomaly(Date time, String src_ip, String dst_ip, String dport, String sport, long bytesin,
				long bytesout, long pkts, long pktsin, long pktsout, long synin, long synackout, long rstin,
				long rstout, long fin, long get, long post) {
			super(time, src_ip, dst_ip, dport, sport, bytesin, bytesout, pkts, pktsin, pktsout, synin, synackout, rstin, rstout,
					fin, get, post);
		}



		public NMAJSONDataAnomaly(Date time, String src_ip, String dst_ip, String dport, String sport, long bytesin,
				long bytesout, long pkts, long pktsin, long pktsout, long synin, long synackout, long rstin,
				long rstout, long get, long post) {
			super(time, src_ip, dst_ip, dport, sport, bytesin, bytesout, pkts, pktsin, pktsout, synin, synackout, rstin, rstout,
					get, post);
		}



		public NMAJSONDataAnomaly(NMAJSONData input) {
			super();
			
			this.time = input.getTime();
			this.src_ip = input.getSrc_ip();
			this.dst_ip = input.getDst_ip();
			this.dport = input.dport;
			this.sport = input.sport;
			this.synin = input.synin;
			this.synackout = input.synackout;
			this.rstin = input.rstin;
			this.rstout = input.rstout;
			this.fin = input.fin;
			
			
			
			if (NMAJSONDataGenerator.r.nextInt(5) != 0) {
				this.setBytesin(Math.round(this.getBytesin() + (NMAJSONDataGenerator.MAX_BYTESIN * getDivergence()) ));
			}else {
				this.setBytesin(Math.max(Math.round(this.getBytesin() - (NMAJSONDataGenerator.MAX_BYTESIN * getDivergence()) ),0));
			}
			if (NMAJSONDataGenerator.r.nextInt(5) != 0) {
				this.setBytesout(Math.round(this.getBytesout() + (NMAJSONDataGenerator.MAX_BYTESOUT * getDivergence())));
			}else {
				this.setBytesout(Math.max(Math.round(this.getBytesout() - (NMAJSONDataGenerator.MAX_BYTESOUT * getDivergence()) ),0));
			}
			
			if (NMAJSONDataGenerator.r.nextInt(5) != 0) {
				this.setPktsin(Math.round(this.getPktsin() + (NMAJSONDataGenerator.MAX_PACKETS_IN * getDivergence()) ));
			}else {
				this.setPktsin(Math.max(Math.round(this.getPktsin() - (NMAJSONDataGenerator.MAX_PACKETS_IN * getDivergence()) ),0));
			}
			if (NMAJSONDataGenerator.r.nextInt(5) != 0) {
				this.setPktsout(Math.round(this.getPktsout() + (NMAJSONDataGenerator.MAX_PACKETS_OUT * getDivergence())));
			}else {
				this.setPktsout(Math.max(Math.round(this.getPktsout() - (NMAJSONDataGenerator.MAX_PACKETS_OUT * getDivergence()) ),0));
			}				
			
			this.setPkts(this.getPktsin() + this.getPktsout());
			
			this.setPost(Math.round((this.getPost() + 1) * NMAJSONDataGenerator.r.nextDouble()));
			this.setGet(Math.round((this.getGet() + 1) * NMAJSONDataGenerator.r.nextDouble()));			
		}



		private double getDivergence() {
			double k = (9 + r.nextInt(1))/10d;
			return k;
		}
	}
	
	
	


}


