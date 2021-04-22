package it.consulthink.oe.flink.packetcount;

import it.consulthink.oe.model.NMAJSONData;
import junit.framework.Assert;
import org.apache.commons.lang3.time.DateUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class TestUtilities {

    public static ArrayList<NMAJSONData> readCSV(String csvFileName) throws FileNotFoundException, ParseException {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        Path path = FileSystems.getDefault().getPath("../common/src/main/resources/" + csvFileName).toAbsolutePath();
        File csv = path.toFile();
        Assert.assertTrue(csv.exists() && csv.isFile());

        ArrayList<NMAJSONData> iterable = new ArrayList<NMAJSONData>();
        Scanner myReader = new Scanner(csv);
        String line = myReader.nextLine();
        while (myReader.hasNextLine()) {
            String[] splitted = myReader.nextLine().split(",");
            NMAJSONData tmp = new NMAJSONData(df.parse(splitted[0]),
                    splitted[1], splitted[2], splitted[3], splitted[4],
                    Long.valueOf(splitted[5]), Long.valueOf(splitted[6]), Long.valueOf(splitted[7]),
                    Long.valueOf(splitted[8]), Long.valueOf(splitted[9]), Long.valueOf(splitted[10]),
                    Long.valueOf(splitted[11]), Long.valueOf(splitted[5]), Long.valueOf(splitted[5]),
                    Long.valueOf(splitted[12]), Long.valueOf(splitted[13]));
            iterable.add(tmp);
        }
        myReader.close();
        return iterable;
    }

    public static FilterFunction<NMAJSONData> getFilterFunction() {
        FilterFunction<NMAJSONData> filter = new FilterFunction<NMAJSONData>() {

            @Override
            public boolean filter(NMAJSONData value) throws Exception {
                Date min = DateUtils.parseDate("2021-03-21  22:59:59", "yyyy-MM-dd HH:mm:ss");
                Date max = DateUtils.parseDate("2021-03-21  23:00:03", "yyyy-MM-dd HH:mm:ss");
                return value.getTime().after(min) && value.getTime().before(max);
            }

        };
        return filter;
    }

}
