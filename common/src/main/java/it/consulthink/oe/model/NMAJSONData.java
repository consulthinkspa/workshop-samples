package it.consulthink.oe.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown = true)
public class NMAJSONData implements Serializable {
	
	
	
/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	//	2021-03-21 23:59:58
	@JsonFormat 
    (shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
	public Date time;
	public String src_ip;
	public String dst_ip;
	public String dport;
	public String sport;
	public long bytesin;
	public long bytesout;
	public long pkts;
	public long pktsin;
	public long pktsout;
	public long synin;
	public long synackout;
	public long rstin;
	public long rstout;
	public long get;
	public long post;

	
	
    @Override
    public String toString() {
        return "NMAJSONData{" +
                "time='" + time + '\'' +
                ", src_ip='" + src_ip + '\'' +
                ", dst_ip='" + dst_ip + '\'' +
                ", dport='" + dport + '\'' +
                ", sport='" + sport + '\'' +
				", bytesin='" + bytesin + '\'' +
				", bytesout='" + bytesout + '\'' +
                ", pkts='" + pkts + '\'' +
                ", pktsin='" + pktsin + '\'' +
                ", pktsout='" + pktsout + '\'' +
                ", synin='" + synin + '\'' +
                ", synackout='" + synackout + '\'' +
                ", rstin='" + rstin + '\'' +
                ", rstout='" + rstout + '\'' +
                ", get='" + get + '\'' +
                ", post='" + post + '\'' +
                '}';
    }
    
    
    
    public NMAJSONData(Date time, String src_ip, String dst_ip, String dport, String sport, long bytesin,
			long bytesout, long pkts, long pktsin, long pktsout, long synin, long synackout, long rstin, long rstout,
			long get, long post) {
		super();
		this.time = time;
		this.src_ip = src_ip;
		this.dst_ip = dst_ip;
		this.dport = dport;
		this.sport = sport;
		this.bytesin = bytesin;
		this.bytesout = bytesout;
		this.pkts = pkts;
		this.pktsin = pktsin;
		this.pktsout = pktsout;
		this.synin = synin;
		this.synackout = synackout;
		this.rstin = rstin;
		this.rstout = rstout;
		this.get = get;
		this.post = post;
	}





	public NMAJSONData() {
		super();
	}



	public Date getTime() {
		return time;
	}

	public void setTime(Date time) {
		this.time = time;
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

	public long getBytesin() {
		return bytesin;
	}

	public void setBytesin(long bytesin) {
		this.bytesin = bytesin;
	}

	public long getBytesout() {
		return bytesout;
	}

	public void setBytesout(long bytesout) {
		this.bytesout = bytesout;
	}

	public long getPkts() {
		return pkts;
	}

	public void setPkts(long pkts) {
		this.pkts = pkts;
	}

	public long getPktsin() {
		return pktsin;
	}

	public void setPktsin(long pktsin) {
		this.pktsin = pktsin;
	}

	public long getPktsout() {
		return pktsout;
	}

	public void setPktsout(long pktsout) {
		this.pktsout = pktsout;
	}

	public long getSynin() {
		return synin;
	}

	public void setSynin(long synin) {
		this.synin = synin;
	}

	public long getSynackout() {
		return synackout;
	}

	public void setSynackout(long synackout) {
		this.synackout = synackout;
	}

	public long getRstin() {
		return rstin;
	}

	public void setRstin(long rstin) {
		this.rstin = rstin;
	}

	public long getRstout() {
		return rstout;
	}

	public void setRstout(long rstout) {
		this.rstout = rstout;
	}

	public long getGet() {
		return get;
	}

	public void setGet(long get) {
		this.get = get;
	}

	public long getPost() {
		return post;
	}

	public void setPost(long post) {
		this.post = post;
	}



	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (bytesin ^ (bytesin >>> 32));
		result = prime * result + (int) (bytesout ^ (bytesout >>> 32));
		result = prime * result + ((dport == null) ? 0 : dport.hashCode());
		result = prime * result + ((dst_ip == null) ? 0 : dst_ip.hashCode());
		result = prime * result + (int) (get ^ (get >>> 32));
		result = prime * result + (int) (pkts ^ (pkts >>> 32));
		result = prime * result + (int) (pktsin ^ (pktsin >>> 32));
		result = prime * result + (int) (pktsout ^ (pktsout >>> 32));
		result = prime * result + (int) (post ^ (post >>> 32));
		result = prime * result + (int) (rstin ^ (rstin >>> 32));
		result = prime * result + (int) (rstout ^ (rstout >>> 32));
		result = prime * result + ((sport == null) ? 0 : sport.hashCode());
		result = prime * result + ((src_ip == null) ? 0 : src_ip.hashCode());
		result = prime * result + (int) (synackout ^ (synackout >>> 32));
		result = prime * result + (int) (synin ^ (synin >>> 32));
		result = prime * result + ((time == null) ? 0 : time.hashCode());
		return result;
	}



	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		NMAJSONData other = (NMAJSONData) obj;
		if (bytesin != other.bytesin)
			return false;
		if (bytesout != other.bytesout)
			return false;
		if (dport == null) {
			if (other.dport != null)
				return false;
		} else if (!dport.equals(other.dport))
			return false;
		if (dst_ip == null) {
			if (other.dst_ip != null)
				return false;
		} else if (!dst_ip.equals(other.dst_ip))
			return false;
		if (get != other.get)
			return false;
		if (pkts != other.pkts)
			return false;
		if (pktsin != other.pktsin)
			return false;
		if (pktsout != other.pktsout)
			return false;
		if (post != other.post)
			return false;
		if (rstin != other.rstin)
			return false;
		if (rstout != other.rstout)
			return false;
		if (sport == null) {
			if (other.sport != null)
				return false;
		} else if (!sport.equals(other.sport))
			return false;
		if (src_ip == null) {
			if (other.src_ip != null)
				return false;
		} else if (!src_ip.equals(other.src_ip))
			return false;
		if (synackout != other.synackout)
			return false;
		if (synin != other.synin)
			return false;
		if (time == null) {
			if (other.time != null)
				return false;
		} else if (!time.equals(other.time))
			return false;
		return true;
	}
    
    
}
