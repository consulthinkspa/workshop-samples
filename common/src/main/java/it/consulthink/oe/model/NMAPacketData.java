package it.consulthink.oe.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.commons.math3.exception.NullArgumentException;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Date;
import java.util.Objects;

@JsonIgnoreProperties(ignoreUnknown = true)
public class NMAPacketData implements Serializable {


public static final String ACTIVE = "ACTIVE";
public static final String CLOSING = "CLOSING";
public static final String OPENING = "OPENING";
public static final String RESET = "RESET";

/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	//	2021-03-21 23:59:58
	@JsonFormat
    (shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")

	public Date time;
	//layer 2
	public byte[] src_mac;
	public byte[] dst_mac;
	public int vlan;
	public int eth_type;

	//layer 3
	public int l3_header_len;
	public int tos;
	public int l3_total_len;
	public int ttl;
	public int ip_proto;
	public String src_ip;
	public String dst_ip;

	// layer 4
	public int sport;
	public int dport;
	public int l4_header_len;
	public int window_size;

	// if ip_proto == 6 (tcp)
	public boolean syn;
	public boolean ack;
	public boolean fin;
	public boolean rst;


	// util
	public int header_size;
	public int payload_size;
	public int size;	// header_size + payload_size



	/***
	 *
	 *
	 *
	 * **/


	public NMAPacketData() {
		super();
	}



	public NMAPacketData(Date time, String src_mac, String dst_mac, int vlan, int eth_type,
						 int l3_header_len, int tos, int l3_total_len, int ttl, int ip_proto, String src_ip,
						 String dst_ip, int sport, int dport, int l4_header_len, int window_size, boolean syn,
						 boolean ack, boolean fin, boolean rst, int size) {

		super();
		this.time = time;
		this.src_mac = parseMAC(src_mac);
		this.dst_mac = parseMAC(dst_mac);
		this.vlan = vlan;
		this.eth_type = eth_type;
		this.l3_header_len = l3_header_len;
		this.tos = tos;
		this.l3_total_len = l3_total_len;
		this.ttl = ttl;
		this.ip_proto = ip_proto;
		this.src_ip = src_ip;
		this.dst_ip = dst_ip;
		this.sport = sport;
		this.dport = dport;
		this.l4_header_len = l4_header_len;
		this.window_size = window_size;
		this.syn = syn;
		this.ack = ack;
		this.fin = fin;
		this.rst = rst;
		this.size = size;

		len_consistency();

	}

	/***
	 *
	 *
	 *
	 * **/



	private void len_consistency(){
		int overhead = 14 + l3_header_len + l4_header_len;
		int payload = size - overhead;

		header_size = (overhead >= 54) ? overhead : 0;

		payload_size = (payload >= 0) ? payload : 0;

	}

//	private Inet4Address parseIP(String ip) {
//		try{
//
//			if(ip == null){
//				throw new NullArgumentException();
//			}
//
//			return (Inet4Address) Inet4Address.getByName(ip);
//
//		} catch (Exception x) {
//			x.printStackTrace();
//			return null;
//		}
//	}

	private byte[] parseMAC(String mac) {

		String[] bytes = mac.split(":");
		byte[] parsed = new byte[bytes.length];

		for (int x = 0; x < bytes.length; x++) {
			BigInteger temp = new BigInteger(bytes[x], 16);
			byte[] raw = temp.toByteArray();
			parsed[x] = raw[raw.length - 1];
		}
		return parsed;


	}

	public String getSessionState() {
		if (syn && !fin && !ack) {
			return OPENING;
		}else if (fin) {
			return CLOSING;
		}else if (rst) {
			return RESET;
		}else {
			return ACTIVE;
		}
	}

	public int getHostHash(){
		if (src_ip == null || dst_ip == null) {
			return -1;
		}
		return src_ip.hashCode() ^ dst_ip.hashCode();
	}

	public int getServiceHash(){
		if (src_ip == null || dst_ip == null || sport <= 0 || dport <= 0) {
			return -1;
		}
		return (src_ip + String.valueOf(sport)).hashCode() ^ (dst_ip + String.valueOf(dport)).hashCode();
	}


	/***
	 *
	 *
	 *
	 * **/



	public Date getTime() {
		return time;
	}

	public byte[] getSrc_mac() {
		return src_mac;
	}

	public byte[] getDst_mac() {
		return dst_mac;
	}

	public int getVlan() {
		return vlan;
	}

	public int getEth_type() {
		return eth_type;
	}

	public int getTos() {
		return tos;
	}

	public int getL3_total_len() {
		return l3_total_len;
	}

	public int getL3_header_len(){
		return l3_header_len;
	}

	public int getTtl() {
		return ttl;
	}

	public int getIp_proto() {
		return ip_proto;
	}

	public String getSrc_ip() {
		return src_ip;
	}

	public String getDst_ip() {
		return dst_ip;
	}

	public int getSport() {
		return sport;
	}

	public int getDport() {
		return dport;
	}

	public int getL4_header_len() {
		return l4_header_len;
	}

	public int getWindow_size() {
		return window_size;
	}

	public boolean isSyn() {
		return syn;
	}

	public boolean isAck() {
		return ack;
	}

	public boolean isFin() {
		return fin;
	}

	public boolean isRst() {
		return rst;
	}

	public int getHeader_size() {
		int overhead = 14 + l3_header_len + l4_header_len;
		return (overhead >= 54) ? overhead : 0;
	}

	public int getPayload_size() {
		int payload = size - header_size;
		return (payload >= 0) ? payload : 0;
	}

	public int getSize() {
		return size;
	}


	/***
	 * this methods are for testing purposes,
	 * should not be used on real traffic because change
	 * the fields of the packet, the headers (break size consistency)
	 * and more...
	 * */

	public void setTime(Date time) {
		this.time = time;
	}

	public void setSrc_mac(byte[] src_mac) {
		this.src_mac = src_mac;
	}

	public void setDst_mac(byte[] dst_mac) {
		this.dst_mac = dst_mac;
	}

	public void setVlan(int vlan) {
		this.vlan = vlan;
	}

	public void setEth_type(int eth_type) {
		this.eth_type = eth_type;
	}


	public void setL3_header_len(int l3_header_len) {
		this.l3_header_len = l3_header_len;
		len_consistency();
	}

	public void setTos(int tos) {
		this.tos = tos;
	}

	public void setL3_total_len(int l3_total_len) {
		this.l3_total_len = l3_total_len;
	}

	public void setTtl(int ttl) {
		this.ttl = ttl;
	}

	public void setIp_proto(int ip_proto) {
		this.ip_proto = ip_proto;
	}

	public void setSrc_ip(String src_ip) {
		this.src_ip = src_ip;
	}

	public void setDst_ip(String dst_ip) {
		this.dst_ip = dst_ip;
	}

	public void setSport(int sport) {
		this.sport = sport;
	}

	public void setDport(int dport) {
		this.dport = dport;
	}

	public void setL4_header_len(int l4_header_len) {
		this.l4_header_len = l4_header_len;
		len_consistency();
	}

	public void setWindow_size(int window_size) {
		this.window_size = window_size;
	}

	public void setSyn(boolean syn) {
		this.syn = syn;
	}

	public void setAck(boolean ack) {
		this.ack = ack;
	}

	public void setFin(boolean fin) {
		this.fin = fin;
	}

	public void setRst(boolean rst) {
		this.rst = rst;
	}

//	public void setHeader_size(long header_size) {
//		this.header_size = header_size;
//	}

//	public void setPayload_size(long payload_size) {
//		this.payload_size = payload_size;
//	}
//
	public void setSize(int size) {
		this.size = size;
		len_consistency();
	}


	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		NMAPacketData that = (NMAPacketData) o;
		return vlan == that.vlan && eth_type == that.eth_type && l3_header_len == that.l3_header_len && tos == that.tos && l3_total_len == that.l3_total_len && ttl == that.ttl && ip_proto == that.ip_proto && sport == that.sport && dport == that.dport && l4_header_len == that.l4_header_len && window_size == that.window_size && syn == that.syn && ack == that.ack && fin == that.fin && rst == that.rst && header_size == that.header_size && payload_size == that.payload_size && size == that.size && Objects.equals(time, that.time) && Arrays.equals(src_mac, that.src_mac) && Arrays.equals(dst_mac, that.dst_mac) && Objects.equals(src_ip, that.src_ip) && Objects.equals(dst_ip, that.dst_ip);
	}

	@Override
	public int hashCode() {
		int result = Objects.hash(time, vlan, eth_type, l3_header_len, tos, l3_total_len, ttl, ip_proto, src_ip, dst_ip, sport, dport, l4_header_len, window_size, syn, ack, fin, rst, header_size, payload_size, size);
		result = 31 * result + Arrays.hashCode(src_mac);
		result = 31 * result + Arrays.hashCode(dst_mac);
		return result;
	}

	/***
	 *
	 *
	 *
	 * ***/



	@Override
	public String toString() {
		return "NMAPacketData{" +
				"time=" + time +
				", src_mac=" + Arrays.toString(src_mac) +
				", dst_mac=" + Arrays.toString(dst_mac) +
				", vlan=" + vlan +
				", eth_type=" + eth_type +
				", l3_header_len=" + l3_header_len +
				", tos=" + tos +
				", l3_total_len=" + l3_total_len +
				", ttl=" + ttl +
				", ip_proto=" + ip_proto +
				", src_ip=" + src_ip +
				", dst_ip=" + dst_ip +
				", sport=" + sport +
				", dport=" + dport +
				", l4_header_len=" + l4_header_len +
				", window_size=" + window_size +
				", syn=" + syn +
				", ack=" + ack +
				", fin=" + fin +
				", rst=" + rst +
				", header_size=" + header_size +
				", payload_size=" + payload_size +
				", size=" + size +
				'}';
	}
}
