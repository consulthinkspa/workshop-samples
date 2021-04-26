package it.consulthink.oe.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.io.Serializable;

@JsonIgnoreProperties(ignoreUnknown = true)

public class Traffic implements Serializable {

    public Long inbound;
    public Long outbound;
    public Long lateral;

    public Traffic(Long inbound, Long outbound, Long lateral) {
        this.inbound = inbound;
        this.outbound = outbound;
        this.lateral = lateral;
    }


    public Traffic(Traffic v1, Traffic v2) {
        this.inbound = v1.getInbound() + v2.getInbound();
        this.outbound = v1.getOutbound() + v2.getOutbound();
        this.lateral = v1.getLateral() + v2.getLateral();
    }


    @Override
    public String toString() {
        return "Traffic{" +
                "inbound=" + inbound +
                ", outbound=" + outbound +
                ", lateral=" + lateral +
                '}';
    }

    public Long getInbound() {
        return inbound;
    }

    public void setInbound(Long inbound) {
        this.inbound = inbound;
    }

    public Long getOutbound() {
        return outbound;
    }

    public void setOutbound(Long outbound) {
        this.outbound = outbound;
    }

    public Long getLateral() {
        return lateral;
    }

    public void setLateral(Long lateral) {
        this.lateral = lateral;
    }
}