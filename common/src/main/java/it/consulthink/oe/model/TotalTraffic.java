package it.consulthink.oe.model;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.flink.api.java.tuple.Tuple2;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class TotalTraffic implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	@JsonFormat 
    (shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
	public Date time = null;
	public Long value = null;
	
	public TotalTraffic() {
		super();
	}
	
	public TotalTraffic(Date time, Long value) {
		super();
		this.time = time;
		this.value = value;
	}
	
	public TotalTraffic(Tuple2<Date, Long> t) {
		this(t.f0, t.f1);
	}
	

	public Date getTime() {
		return time;
	}
	public void setTime(Date time) {
		this.time = time;
	}
	public Long getValue() {
		return value;
	}
	public void setValue(Long value) {
		this.value = value;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((time == null) ? 0 : time.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
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
		TotalTraffic other = (TotalTraffic) obj;
		if (time == null) {
			if (other.time != null)
				return false;
		} else if (!time.equals(other.time))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}
	@Override
	public String toString() {
		return "TotalTraffic [time=" + time + ", value=" + value + "]";
	}
	
	public static List<TotalTraffic> transform(List<Tuple2<Date, Long>> input){
		List<TotalTraffic> result = null;
		if (input != null) {
			result = new ArrayList<TotalTraffic>();
			for (Tuple2<Date, Long> i : input) {
				result.add(new TotalTraffic(i));
			}
		}
		return result;
	}
	
}

