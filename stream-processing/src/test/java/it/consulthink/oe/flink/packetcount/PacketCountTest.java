package it.consulthink.oe.flink.packetcount;

import static org.junit.Assert.*;

import org.junit.Test;

import junit.framework.Assert;

public class PacketCountTest {

	@Test
	public void testGetNetworkHash() {
		PacketCount p1 = new PacketCount();
		p1.setSrc_ip("192.168.1.25");
		p1.setSport("14546");
		p1.setDst_ip("213.61.202.114");
		p1.setDport("443");
		
		PacketCount p2 = new PacketCount();
		p2.setSrc_ip("213.61.202.114");
		p2.setSport("443");
		p2.setDst_ip("192.168.1.25");
		p2.setDport("14546");
		
		Assert.assertEquals(p1.getNetworkHash(), p2.getNetworkHash());
		
		PacketCount p3 = new PacketCount();
		p3.setSrc_ip("192.168.1.25");
		p3.setSport("443");
		p3.setDst_ip("213.61.202.114");
		p3.setDport("14546");
		
		Assert.assertFalse(p1.getNetworkHash().equals(p3.getNetworkHash()));
		
		PacketCount p4 = new PacketCount();
		Assert.assertNotNull(p4.getNetworkHash());
		Assert.assertFalse(p1.getNetworkHash().equals(p4.getNetworkHash()));
	}

}
