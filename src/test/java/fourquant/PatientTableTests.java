package fourquant;

/**
 * Created by mader on 5/6/16.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class PatientTableTests extends Serializable {
    final static String fijiPath = "/Applications/Fiji.app/Contents/";
    final static ImageJSettings ijs = new USBImageJSettings(fijiPath,false,false,false);
    final static int bindPort = 11112;
    final static String bindAddress = "localhost";

}
