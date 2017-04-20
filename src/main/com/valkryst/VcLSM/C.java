package main.com.valkryst.VcLSM;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Created by jiapengzhu on 2017-04-10.
 */
public class C {
    public static final String DILIMETER = "+";
    public static final String V = "value";
    public static final String K = "key";
    public static final String TIME = "time";
    public static final String L = "LevelDB";
    public static final String C = "cLSM";
    public final static int DELAY = 60;
    public static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS");
    public static final Logger logger = LogManager.getLogger();
}
