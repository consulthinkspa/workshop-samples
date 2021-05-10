package com.dellemc.oe.model;

import it.consulthink.oe.model.Session;
import junit.framework.Assert;
import org.apache.flink.api.java.tuple.Tuple5;
import org.junit.Test;

import java.util.Date;

public class SessionTest {

    @Test
    public void testHashId(){


        Tuple5<Date, String, String, Integer, Integer> first = Tuple5.of(new Date(), "213.61.202.114" , "8.8.8.8", 26481, 53 );
        Session firstSession = new Session(first);

        Tuple5<Date, String, String, Integer, Integer> second = Tuple5.of(new Date(), "8.8.8.8" , "213.61.202.114", 53, 26481 );
        Session secondSession = new Session(second);

        Assert.assertEquals(firstSession.hashId, secondSession.hashId);

    }


    @Test
    public void testHostHash(){


        Tuple5<Date, String, String, Integer, Integer> first = Tuple5.of(new Date(), "213.61.202.114" , "8.8.8.8", 26481, 53 );
        Session firstSession = new Session(first);

        Tuple5<Date, String, String, Integer, Integer> second = Tuple5.of(new Date(), "8.8.8.8" , "213.61.202.114", 53, 2641181 );
        Session secondSession = new Session(second);

        Assert.assertEquals(firstSession.hostHash, secondSession.hostHash);

    }



}
