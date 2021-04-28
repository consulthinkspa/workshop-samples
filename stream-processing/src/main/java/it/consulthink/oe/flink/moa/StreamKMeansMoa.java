package it.consulthink.oe.flink.moa;
import com.yahoo.labs.samoa.instances.Attribute;
import com.yahoo.labs.samoa.instances.DenseInstance;

import moa.cluster.Cluster;
import moa.cluster.Clustering;
import moa.clusterers.streamkm.StreamKM;
import moa.core.AutoExpandVector;

import java.time.Clock;
import java.util.Iterator;

import org.apache.flink.api.java.tuple.Tuple3;


public class StreamKMeansMoa {

    public StreamKMeansMoa() {}

    public static void run() {
        StreamKM streamKM = new StreamKM();
        streamKM.numClustersOption.setValue(5); // default setting
        streamKM.lengthOption.setValue(100000); // changed option from width to length
        streamKM. resetLearning(); // UPDATED CODE LINE !!!

        for (int i = 0; i < 150000; i++) {
            streamKM.trainOnInstanceImpl(randomInstance(5));
        }

        Clustering result = streamKM.getClusteringResult();

        System.out.println("RESULT TO STRING: " + result.toString());




        
        AutoExpandVector<Cluster> clusteringCopy = result.getClusteringCopy();
        final DenseInstance randomInstance = randomInstance(5);
        
        double maxProbability = 0;
        Cluster assignedCluster = null;
        int i = 0;
        for (Cluster cluster : clusteringCopy) {
        	double inclusionProbability = cluster.getInclusionProbability(randomInstance);
        	if (inclusionProbability > maxProbability) {
        		maxProbability = inclusionProbability; 
        		assignedCluster = cluster;
        	}
        	System.out.println(i);
        	if (inclusionProbability >= 1) {
        		break;
        	}
        	
        	i++;
		}

        
        System.out.println("size = " + result.size());
        System.out.println("dimension = " + result.dimension());
        
        if (randomInstance != null && assignedCluster != null) {
//        	System.out.println("randomInstance = " + randomInstance);
        	
            System.out.println("maxProbability = " + maxProbability);
            System.out.println("assignedCluster = " + assignedCluster.getId());
            System.out.println("assignedCluster = " + assignedCluster.getInfo());
        }


        
        

    }


    public static DenseInstance randomInstance(int size) {
        DenseInstance instance = new DenseInstance(size);
        for (int idx = 0; idx < size; idx++) {
            instance.setValue(idx, Math.random());
        }
        return instance;
    }


}
