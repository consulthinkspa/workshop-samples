package it.consulthink.oe.model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown = true)
public class NMAJSONData implements Serializable {
	
	
	
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
        return "JSONData{" +
                "time='" + time + '\'' +
                ", src_ip='" + src_ip + '\'' +
                ", dst_ip='" + dst_ip + '\'' +
                ", dport='" + dport + '\'' +
                ", sport='" + sport + '\'' +
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
    
    
}
