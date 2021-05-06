/**
 * 
 */
package it.consulthink.oe.model;

import java.io.Serializable;
import java.util.Arrays;

import org.apache.flink.api.java.tuple.Tuple3;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import moa.cluster.Cluster;

/**
 * @author svetr
 *
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class Anomaly implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public NMAJSONData data;
	public Double maxProbability;
	public double[] assignedClusterCenter;

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
		this.assignedClusterCenter = assignedCluster != null ? assignedCluster.getCenter() : null;
	}
	
	public Anomaly(Tuple3<NMAJSONData, Double, Cluster> t) {
		this(t.f0, t.f1, t.f2);
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

	public double[] getAssignedClusterCenter() {
		return assignedClusterCenter;
	}

	public void setAssignedClusterCenter(double[] assignedClusterCenter) {
		this.assignedClusterCenter = assignedClusterCenter;
	}

	@Override
	public String toString() {
		return "Anomaly [data=" + data + ", maxProbability=" + maxProbability + ", assignedClusterCenter="
				+ Arrays.toString(assignedClusterCenter) + "]";
	}	
	

}
