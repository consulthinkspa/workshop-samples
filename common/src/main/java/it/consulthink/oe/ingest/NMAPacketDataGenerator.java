/**
 * 
 */
package it.consulthink.oe.ingest;

import it.consulthink.oe.model.NMAJSONData;
import it.consulthink.oe.model.NMAPacketData;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * @author svetr
 *
 */
public class NMAPacketDataGenerator {
	


	public static final int MEAN_SIZE = 700;
	public static final int MAX_SIZE = 1500; //MTU

	public static final int MEAN_L3_HL = 20;
	public static final int MAX_L3_HL = 60;

	public static final int MEAN_L4_HL = 20;
	public static final int MAX_L4_HL = 60;

	public static final int MEAN_WIN_SIZE = 131071;
	public static final int MAX_WIN_SIZE = (int) Math.pow(MEAN_WIN_SIZE,4);


	public static final Random r = new Random(System.currentTimeMillis());
	
	private static final HashMap<String, Long> ips = new HashMap<String, Long>();
	private static final HashMap<String, Long> ipsLocal = new HashMap<String, Long>();



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


	private synchronized static String generateRandomLocalIP() {
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
	
	private synchronized static String generateRandomIP() {
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



	private static int generateL3_header_len(){
		long result = MEAN_L3_HL;
		if (r.nextBoolean()) {
			int maxRandomInt = Math.floorDiv(MAX_L3_HL - (int)MEAN_L3_HL, 2);
			result = result + r.nextInt(maxRandomInt);
		}else {
			int maxRandomInt = Math.floorDiv((int)MEAN_L3_HL, 2);
			result = result - r.nextInt(maxRandomInt);
		}
		return (int) Math.max(Math.min(result, MAX_L3_HL),0);
	}

	private static int generateL4_header_len(){
		long result = MEAN_L4_HL;
		if (r.nextBoolean()) {
			int maxRandomInt = Math.floorDiv(MAX_L4_HL - (int)MEAN_L4_HL, 2);
			result = result + r.nextInt(maxRandomInt);
		}else {
			int maxRandomInt = Math.floorDiv((int)MEAN_L4_HL, 2);
			result = result - r.nextInt(maxRandomInt);
		}
		return (int) Math.max(Math.min(result, MAX_L4_HL),0);
	}

	private static int generateWindowSize(){
		long result = MEAN_WIN_SIZE;
		if (r.nextBoolean()) {
			int maxRandomInt = Math.floorDiv(MAX_WIN_SIZE - (int)MEAN_WIN_SIZE, 2);
			result = result + r.nextInt(maxRandomInt);
		}else {
			int maxRandomInt = Math.floorDiv((int)MEAN_WIN_SIZE, 2);
			result = result - r.nextInt(maxRandomInt);
		}
		return (int) Math.max(Math.min(result, MAX_WIN_SIZE),0);
	}


	private static int generateSize(){
		long result = MEAN_SIZE;
		if (r.nextBoolean()) {
			int maxRandomInt = Math.floorDiv(MAX_SIZE - (int)MEAN_SIZE, 2);
			result = result + r.nextInt(maxRandomInt);
		}else {
			int maxRandomInt = Math.floorDiv((int)MEAN_SIZE, 2);
			result = result - r.nextInt(maxRandomInt);
		}
		return (int) Math.max(Math.min(result, MAX_SIZE),0);
	}


	private static int generateTos(){
		return 0;
	}

	private static int generateTtl(){
		return 100;
	}

	private static int generateIpProto(){
		return r.nextBoolean() ? 6 : 17;
	}


	public static Stream<NMAPacketData> generateInfiniteStream(Collection<String> myIps, Collection<String> macs){
		
		return Stream.generate(new Supplier<NMAPacketData>() {

			@Override
			public NMAPacketData get() {

				return r.nextInt(95) == 0 ? generateAnomaly(myIps, macs) : generateStandard(myIps, macs);
			}

		});
	}
	
	public static Stream<NMAPacketData> generateInfiniteStreamNoAnomaly(Collection<String> myIps,
																		Collection<String> macs){
		
		return Stream.generate(new Supplier<NMAPacketData>() {
			@Override
			public NMAPacketData get() {
				try {
					Thread.sleep(70 + r.nextInt(30));
				} catch (Throwable e) {
					//NOP
				}				
				return generateStandard(myIps,macs);
			}
			
		});
	}	
	
		
	public static NMAPacketData generateAnomaly(Collection<String> myIps, Collection<String> macs) {

		NMAPacketData result = new NMAPacketDataAnomaly(generateStandard(myIps,macs));
		
		return result;
		
		
	}


	public static NMAPacketData generateStandard(Collection<String> ips, Collection<String> macs) {
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
		else if (macs == null || macs.isEmpty()) {
			String str = ""
					+ "00:d7:8f:29:ea:cc,"
					+ "00:00:5e:00:01:72,"
					+ "4e:d3:5e:0a:52:68,"
					+ "00:00:5e:00:01:76,"
					+ "00:00:5e:00:01:73,"
					+ "01:00:5e:00:00:12,"
					+ "00:00:5e:00:01:75,"
					+ "00:00:5e:00:01:74,"
					+ "00:00:5e:00:01:78,"
					+ "00:00:5e:00:01:7a";
			String[] a = str.split(",");
			macs = Arrays.asList(a);
		}

		List<String> myIps = new ArrayList<String>(ips);
		List<String> myMacs = new ArrayList<String>(macs);

		NMAPacketData result = null;
		
		Date time = new Date(System.currentTimeMillis());

		// generate Layer 2 data
		String src_mac = myMacs.get(r.nextInt(myMacs.size()));
		String dst_mac = myMacs.get(r.nextInt(myMacs.size()));

		int vlan = r.nextInt(10);
		int	eth_type = 2048;


		// generate Layer 3 data
		boolean lateral = r.nextInt(100) == 0;

		int l3_header_len = generateL3_header_len();
		int tos = generateTos();

		int ttl = generateTtl();
		int ip_proto = generateIpProto();

		String src_ip;
		String dst_ip;

		if (lateral) {
			src_ip = generateRandomLocalIP();
			dst_ip = generateRandomLocalIP();
		} else {
			boolean isSrcIpMyIp = r.nextBoolean();
			src_ip = isSrcIpMyIp ? myIps.get(r.nextInt(myIps.size())) : generateRandomIP();
			dst_ip = !isSrcIpMyIp ? myIps.get(r.nextInt(myIps.size())) : generateRandomIP();
		}

		// generate layer 4 data
		boolean isDportWellKnown = r.nextBoolean();
		int dport = new Integer(isDportWellKnown ? generateWellKnownPort() : generateDinamicPort());
		int sport = new Integer(!isDportWellKnown ? generateWellKnownPort(): generateDinamicPort());

		int l4_header_len = generateL4_header_len();
		int window_size = generateWindowSize();


		boolean syn = false;
		boolean ack = false;
		boolean fin = false;
		boolean rst = false;

		int chance = r.nextInt(100);

		if (chance <= 20) {
			syn = true;
		} else if (chance <= 70) {
			ack = true;
		} else if (chance <= 90) {
			fin = true;
		} else {
			rst = true;
		}

		int size = generateSize();
		int l3_total_len = size - 14;

		result = new NMAPacketData( time, src_mac, dst_mac, vlan, eth_type, l3_header_len, tos, l3_total_len, ttl,
				ip_proto, src_ip, dst_ip, sport, dport, l4_header_len, window_size, syn, ack, fin, rst, size);


		return result;
	}





	public static class NMAPacketDataAnomaly extends NMAPacketData{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		
		

		public NMAPacketDataAnomaly() {
			super();
		}

		public NMAPacketDataAnomaly(Date time, String src_mac, String dst_mac, int vlan, int eth_type,
									int l3_header_len, int tos, int l3_total_len, int ttl, int ip_proto,
									String src_ip, String dst_ip, int sport, int dport, int l4_header_len,
									int window_size, boolean syn, boolean ack, boolean fin, boolean rst, int size) {

			super(time, src_mac, dst_mac, vlan, eth_type, l3_header_len, tos, l3_total_len, ttl, ip_proto,
					src_ip, dst_ip, sport, dport, l4_header_len, window_size, syn, ack, fin, rst, size);
		}






		public NMAPacketDataAnomaly(NMAPacketData input) {
			super();
			
			this.time = input.getTime();

			this.src_mac = input.src_mac;
			this.dst_mac = input.dst_mac;
			this.vlan = input.vlan;
			this.eth_type = input.eth_type;


			this.l3_header_len = input.l3_header_len;
			this.tos = input.tos;
			this.l3_total_len = input.l3_total_len;
			this.ttl = input.ttl;
			this.ip_proto = input.ip_proto;
			this.src_ip = input.getSrc_ip();
			this.dst_ip = input.getDst_ip();

			this.dport = input.dport;
			this.sport = input.sport;
			this.l4_header_len = input.l4_header_len;
			this.window_size = input.window_size;
			this.syn = input.syn;
			this.ack = input.ack;
			this.fin = input.fin;
			this.rst = input.rst;


			this.header_size = input.header_size;
			this.payload_size = input.payload_size;
			this.size = input.size;
			
			
			if (NMAPacketDataGenerator.r.nextInt(5) != 0) {
				this.setL3_header_len((int) Math.round(this.getL3_header_len() + (NMAPacketDataGenerator.MAX_L3_HL * getDivergence()) ));
			}else {
				this.setL3_header_len((int) Math.max(Math.round(this.getL3_header_len() - (NMAPacketDataGenerator.MAX_L3_HL * getDivergence()) ),0));
			}
			if (NMAPacketDataGenerator.r.nextInt(5) != 0) {
				this.setL4_header_len((int) Math.round(this.getL4_header_len() + (NMAPacketDataGenerator.MAX_L4_HL * getDivergence())));
			}else {
				this.setL4_header_len((int) Math.max(Math.round(this.getL4_header_len() - (NMAPacketDataGenerator.MAX_L4_HL * getDivergence()) ),0));
			}
			
			if (NMAPacketDataGenerator.r.nextInt(5) != 0) {
				this.setWindow_size((int) Math.round(this.getWindow_size() + (NMAPacketDataGenerator.MAX_WIN_SIZE * getDivergence()) ));
			}else {
				this.setWindow_size((int) Math.max(Math.round(this.getWindow_size() - (NMAPacketDataGenerator.MAX_WIN_SIZE * getDivergence()) ),0));
			}

			if (NMAPacketDataGenerator.r.nextInt(5) != 0) {
				this.setSize((int) Math.round(this.getSize() + (NMAPacketDataGenerator.MAX_SIZE * getDivergence()) ));
			}else {
				this.setSize((int) Math.max(Math.round(this.getSize() - (NMAPacketDataGenerator.MAX_SIZE * getDivergence()) ),0));
			}

			/**
			 * here can be added more anomaly conditions
			 * **/


		}



		private double getDivergence() {
			double k = (9 + r.nextInt(1))/10d;
			return k;
		}
	}
	
	
	


}


