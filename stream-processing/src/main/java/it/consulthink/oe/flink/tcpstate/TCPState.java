package it.consulthink.oe.flink.tcpstate;

/**
 *
 */


import java.io.Serializable;

import it.consulthink.oe.model.NMAJSONData;

/**
 * @author selia
 *
 */
public class TCPState implements Serializable{

    private long syn;
    private long synAck;

    public String src_ip;
    public String dst_ip;
    public String dport;
    public String sport;

    public TCPState() {
        super();
    }



    public TCPState(long syn, long synAck, String src_ip, String dst_ip, String dport, String sport) {
        super();
        this.syn = syn;
        this.synAck = synAck;
        this.src_ip = src_ip;
        this.dst_ip = dst_ip;
        this.dport = dport;
        this.sport = sport;
    }



    public TCPState(NMAJSONData data) {
        super();
        this.src_ip = data.src_ip;
        this.dst_ip = data.dst_ip;
        this.dport = data.dport;
        this.sport = data.sport;

        this.syn = data.getSynin();
        this.synAck = data.getSynackout();


    }



    public long getSyn() {
        return syn;
    }

    public void setSyn(long syn) {
        this.syn = syn;
    }

    public long getSynAck() {
        return synAck;
    }

    public void setSynIn(long synAck) {
        this.synAck = synAck;
    }


    public String getNetworkHash() {
//    	return "".concat(src_ip).concat(sport).concat(dst_ip).concat(dport);
        return String.valueOf("".concat(src_ip).concat(sport).hashCode() ^ "".concat(dst_ip).concat(dport).hashCode());
    }

}
