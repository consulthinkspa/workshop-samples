package it.consulthink.oe.flink.moa;
import com.yahoo.labs.samoa.instances.DenseInstance;
import moa.cluster.Clustering;
import moa.clusterers.streamkm.StreamKM;

import java.time.Clock;


public class StreamKMeansMoa {

    public StreamKMeansMoa() {}

    public static void run() {
        StreamKM streamKM = new StreamKM();
        streamKM.numClustersOption.setValue(5); // default setting
        streamKM.lengthOption.setValue(100000); // changed option from width to length
        streamKM. resetLearning(); // UPDATED CODE LINE !!!

        for (int i = 0; i < 150000; i++) {
            streamKM.trainOnInstanceImpl(randomInstance(2));
        }

        Clustering result = streamKM.getClusteringResult();

        System.out.println("RESULT TO STRING: " + result.toString());




        System.out.println("size = " + result.size());
        System.out.println("dimension = " + result.dimension());

    }


    public static DenseInstance randomInstance(int size) {
        DenseInstance instance = new DenseInstance(size);
        for (int idx = 0; idx < size; idx++) {
            instance.setValue(idx, Math.random());
        }
        return instance;
    }


}
