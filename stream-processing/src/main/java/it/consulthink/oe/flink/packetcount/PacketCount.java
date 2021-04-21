/**
 * 
 */
package it.consulthink.oe.flink.packetcount;

import java.io.Serializable;

import it.consulthink.oe.model.NMAJSONData;

/**
 * @author svetr
 *
 */
public class PacketCount implements Serializable {

	private long count;

	public String src_ip;
	public String dst_ip;
	public String dport;
	public String sport;

	public PacketCount(long count, String src_ip, String dst_ip, String dport, String sport) {
		super();
		this.count = count;
		this.src_ip = src_ip;
		this.dst_ip = dst_ip;
		this.dport = dport;
		this.sport = sport;
	}

	public PacketCount(NMAJSONData data) {
		super();
		this.src_ip = data.src_ip;
		this.dst_ip = data.dst_ip;
		this.dport = data.dport;
		this.sport = data.sport;

		this.count = data.getPkts();
	}

	public String getSrc_ip() {
		return src_ip;
	}

	public void setSrc_ip(String src_ip) {
		this.src_ip = src_ip;
	}

	public String getDst_ip() {
		return dst_ip;
	}

	public void setDst_ip(String dst_ip) {
		this.dst_ip = dst_ip;
	}

	public String getDport() {
		return dport;
	}

	public void setDport(String dport) {
		this.dport = dport;
	}

	public String getSport() {
		return sport;
	}

	public void setSport(String sport) {
		this.sport = sport;
	}

	public PacketCount() {
		super();
	}

	public long getCount() {
		return count;
	}

	public void setCount(long count) {
		this.count = count;
	}

	public String getNetworkHash() {
//    	return "".concat(src_ip).concat(sport).concat(dst_ip).concat(dport);
		if (src_ip == null || sport == null || dst_ip == null || dport == null)
			return "";
    	return String.valueOf( src_ip.concat(sport).hashCode() ^ dst_ip.concat(dport).hashCode() );
    }

}
