package fourquant;

import fourquant.imagej.ImageJSettings;
import fourquant.pacs.api.PatientTable;
import fourquant.pacs.patients;
import fourquant.riqae.USBImageJSettings;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.io.Serializable;

import static org.junit.Assert.assertEquals;

/**
 * Created by mader on 5/9/16.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class FakePluginTests implements Serializable {
    final static String fijiPath = "/Applications/Fiji.app/Contents/";
    final static ImageJSettings ijs = new USBImageJSettings(fijiPath,false,false,false);

    // get the default values from the communication singleton class
    final static String bindPort = patients.PacsCommunicationSingleton.port();
    final static String bindAddress = patients.PacsCommunicationSingleton.server();
    final static String bindName = patients.PacsCommunicationSingleton.bind();
    final static String userName = patients.PacsCommunicationSingleton.userName();
    static {
        // register the fake plugin
    }
    PatientTable pt = PatientTable.create_from_csv(FakePluginTests.class.getResource("/single_name.csv").getPath(),
            ijs,bindPort,bindAddress,userName,bindName);

    @Test
    public void createFakeTableTest() {
        assertEquals("Should only be one element",pt.count(),1);
    }

}