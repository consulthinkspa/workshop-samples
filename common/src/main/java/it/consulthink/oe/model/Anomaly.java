/**
 * 
 */
package it.consulthink.oe.model;

import java.io.Serializable;

import org.apache.flink.api.java.tuple.Tuple3;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import moa.cluster.Cluster;

/**
 * @author svetr
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Anomaly implements Serializable{
	
	public NMAJSONData data;
	public Double maxProbability;
	public Cluster assignedCluster;

	/**
	 * 
	 */
	public Anomaly() {
		super();
	}

	public Anomaly(NMAJSONData data, Double maxProbability, Cluster assignedCluster) {
		super();
		this.data = data;
		this.maxProbability = maxProbability;
		this.assignedCluster = assignedCluster;
	}
	
	public Anomaly(Tuple3<NMAJSONData, Double, Cluster> t) {
		super();
		this.data = t.f0;
		this.maxProbability = t.f1;
		this.assignedCluster = t.f2;
	}

	public NMAJSONData getData() {
		return data;
	}

	public void setData(NMAJSONData data) {
		this.data = data;
	}

	public Double getMaxProbability() {
		return maxProbability;
	}

	public void setMaxProbability(Double maxProbability) {
		this.maxProbability = maxProbability;
	}

	public Cluster getAssignedCluster() {
		return assignedCluster;
	}

	public void setAssignedCluster(Cluster assignedCluster) {
		this.assignedCluster = assignedCluster;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((data == null) ? 0 : data.hashCode());
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
		Anomaly other = (Anomaly) obj;
		if (data == null) {
			if (other.data != null)
				return false;
		} else if (!data.equals(other.data))
			return false;
		return true;
	}	
	

}
