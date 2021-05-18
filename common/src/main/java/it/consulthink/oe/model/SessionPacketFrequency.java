package it.consulthink.oe.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.flink.api.java.tuple.Tuple5;

import java.net.Inet4Address;
import java.util.Date;

@JsonIgnoreProperties(ignoreUnknown = true)
public class SessionPacketFrequency extends Session{

	// client to server: c2s
	public int c2sPacketsPerSecond;
	public long c2sBurstDuration;
	public int c2sBurstSize;
	public double c2sBurstMeanPktSize;

	// server to client: s2c
	public int s2cPacketsPerSecond;
	public long s2cBurstDuration;
	public int s2cBurstSize;
	public double s2cBurstMeanPktSize;


	// total
	public int packetsPerSecond;

	public SessionPacketFrequency() {
		super();
	}

	public SessionPacketFrequency(Date time, String srcIp, String dstIp, int srcPort, int dstPort,
								  int c2sPacketsPerSecond, int s2cPacketsPerSecond, int packetsPerSecond) {
		super(time, srcIp, dstIp, srcPort, dstPort);
		this.c2sPacketsPerSecond = c2sPacketsPerSecond;
		this.s2cPacketsPerSecond = s2cPacketsPerSecond;
		this.packetsPerSecond = packetsPerSecond;
	}

	public SessionPacketFrequency(Date time, Inet4Address srcIp, Inet4Address dstIp, int srcPort, int dstPort,
								  int c2sPacketsPerSecond, int s2cPacketsPerSecond, int packetsPerSecond) {
		super(time, srcIp, dstIp, srcPort, dstPort);
		this.c2sPacketsPerSecond = c2sPacketsPerSecond;
		this.s2cPacketsPerSecond = s2cPacketsPerSecond;
		this.packetsPerSecond = packetsPerSecond;
	}


	public SessionPacketFrequency(Date time, Inet4Address srcIp, Inet4Address dstIp, int srcPort, int dstPort,
								  long c2sBurstDuration, int c2sBurstSize, double c2sBurstMeanPktSize,
								  long s2cBurstDuration, int s2cBurstSize, double s2cBurstMeanPktSize) {

		super(time, srcIp, dstIp, srcPort, dstPort);
		this.c2sBurstDuration = c2sBurstDuration;
		this.c2sBurstSize = c2sBurstSize;
		this.c2sBurstMeanPktSize = c2sBurstMeanPktSize;

		this.s2cBurstDuration = s2cBurstDuration;
		this.s2cBurstSize = s2cBurstSize;
		this.s2cBurstMeanPktSize = s2cBurstMeanPktSize;

	}



	public SessionPacketFrequency(Tuple5<Date, String, String, Integer, Integer> t) {
		super(t);
	}



	public int getC2sPacketsPerSecond() {
		return c2sPacketsPerSecond;
	}

	public void setC2sPacketsPerSecond(int c2sPacketsPerSecond) {
		this.c2sPacketsPerSecond = c2sPacketsPerSecond;
	}

	public long getC2sBurstDuration() {
		return c2sBurstDuration;
	}

	public void setC2sBurstDuration(int c2sBurstDuration) {
		this.c2sBurstDuration = c2sBurstDuration;
	}

	public int getC2sBurstSize() {
		return c2sBurstSize;
	}

	public void setC2sBurstSize(int c2sBurstSize) {
		this.c2sBurstSize = c2sBurstSize;
	}

	public double getC2sBurstMeanPktSize() {
		return c2sBurstMeanPktSize;
	}

	public void setC2sBurstMeanPktSize(int c2sBurstMeanPktSize) {
		this.c2sBurstMeanPktSize = c2sBurstMeanPktSize;
	}

	public int getS2cPacketsPerSecond() {
		return s2cPacketsPerSecond;
	}

	public void setS2cPacketsPerSecond(int s2cPacketsPerSecond) {
		this.s2cPacketsPerSecond = s2cPacketsPerSecond;
	}

	public long getS2cBurstDuration() {
		return s2cBurstDuration;
	}

	public void setS2cBurstDuration(int s2cBurstDuration) {
		this.s2cBurstDuration = s2cBurstDuration;
	}

	public int getS2cBurstSize() {
		return s2cBurstSize;
	}

	public void setS2cBurstSize(int s2cBurstSize) {
		this.s2cBurstSize = s2cBurstSize;
	}

	public double getS2cBurstMeanPktSize() {
		return s2cBurstMeanPktSize;
	}

	public void setS2cBurstMeanPktSize(int s2cBurstMeanPktSize) {
		this.s2cBurstMeanPktSize = s2cBurstMeanPktSize;
	}


	@Override
	public String toString() {
		return "SessionPacketFrequency{" +
				"time=" + time +
				", srcIp=" + client +
				", dstIp=" + server +
				", srcPort=" + srcPort +
				", dstPort=" + dstPort +
				", hashId=" + hashId +
				", hostHash=" + hostHash +
				", c2sPacketsPerSecond=" + c2sPacketsPerSecond +
				", c2sBurstDuration=" + c2sBurstDuration +
				", c2sBurstSize=" + c2sBurstSize +
				", c2sBurstMeanPktSize=" + c2sBurstMeanPktSize +
				", s2cPacketsPerSecond=" + s2cPacketsPerSecond +
				", s2cBurstDuration=" + s2cBurstDuration +
				", s2cBurstSize=" + s2cBurstSize +
				", s2cBurstMeanPktSize=" + s2cBurstMeanPktSize +
				'}';
	}


}

