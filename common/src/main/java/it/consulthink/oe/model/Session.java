package it.consulthink.oe.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.commons.math3.exception.NullArgumentException;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;


import java.io.Serializable;
import java.net.Inet4Address;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Session implements Serializable{
	/**
	 *
	 */
	private static final long serialVersionUID = 1L;
	@JsonFormat
    (shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
	public Date time = null;

	public Inet4Address srcIp;
	public Inet4Address dstIp;
	public Integer srcPort;
	public Integer dstPort;

	public int hashId;

	public int hostHash;

	/**Può essere estesa con altri campi di interesse**/

	public Session() {
		super();
	}

	public Session(Date time, String srcIp, String dstIp, int srcPort, int dstPort) {
		super();

		this.time = time;
		this.srcIp = parseIP(srcIp);
		this.dstIp = parseIP(dstIp);
		this.srcPort = srcPort;
		this.dstPort = dstPort;

		this.hashId = hashCode();


	}


	public Session(Tuple5<Date, String, String, Integer, Integer> t) {
		this(t.f0, t.f1, t.f2, t.f3, t.f4);
	}
	

	public Date getTime() {
		return time;
	}
	public void setTime(Date time) {
		this.time = time;
	}
	public Inet4Address getSrcIp() {
		return srcIp;
	}
	public void setSrcIp(String value) {
		this.srcIp = parseIP(value);
	}

	public Inet4Address getDstIp() {
		return dstIp;
	}
	public void setDstIp(String value) {
		this.dstIp = parseIP(value);
	}


	public int getSrcPort() {
		return srcPort;
	}

	public void setSrcPort(int srcPort) {
		this.srcPort = srcPort;
	}

	public int getDstPort() {
		return dstPort;
	}

	public void setDstPort(int dstPort) {
		this.dstPort = dstPort;
	}

	public long getHashId() {
		return hashId;
	}


	private Inet4Address parseIP(String ip) {
		try{

			if(ip == null){
				throw new NullArgumentException();
			}

			return (Inet4Address) Inet4Address.getByName(ip);

		} catch (Exception x) {
			x.printStackTrace();
			return null;
		}
	}


	@Override
	public int hashCode() {
		if (srcIp == null || srcPort == null  || dstIp == null || dstPort == null) {
			return -1;
		}
		return (srcIp + srcPort.toString()).hashCode() ^ (dstIp + dstPort.toString()).hashCode();
	}


	public int hostHashCode() {
		if (srcIp == null || srcPort == null  || dstIp == null || dstPort == null) {
			return -1;
		}
		return srcIp.hashCode() ^ dstIp.hashCode();
	}



	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Session other = (Session) obj;

		if (time == null) {
			if (other.time != null)
				return false;
		} else if (!time.equals(other.time))
			return false;

		if (srcIp == null) {
			if (other.srcIp != null)
				return false;
		} else if (!srcIp.equals(other.srcIp))
			return false;

		if (dstIp == null) {
			if (other.dstIp != null)
				return false;
		} else if (!dstIp.equals(other.dstIp))
			return false;


		if (srcPort == null) {
			if (other.srcPort != null)
				return false;
		} else if (!srcPort.equals(other.srcPort))
			return false;
		if (dstPort == null) {
			if (other.dstPort != null)
				return false;
		} else if (!dstPort.equals(other.dstPort))
			return false;

		return true;
	}

	@Override
	public String toString() {
		return "Session {" +
				"time=" + time +
				", srcIp=" + srcIp +
				", dstIp=" + dstIp +
				", srcPort=" + srcPort +
				", dstPort=" + dstPort +
				", hashId=" + hashId +
				'}';
	}

	public static List<Session> transform(List<Tuple5<Date, String, String, Integer, Integer>> input){
		List<Session> result = null;
		if (input != null) {
			result = new ArrayList<Session>();
			for (Tuple5<Date, String, String, Integer, Integer> i : input) {
				result.add(new Session(i));
			}
		}
		return result;
	}
	
}

