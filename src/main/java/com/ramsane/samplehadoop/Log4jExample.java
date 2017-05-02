
package com.ramsane.samplehadoop;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

/**
 *
 * @author ram
 */
public class Log4jExample {
    static Logger logger = Logger.getLogger(Log4jExample.class);
    public static void main(String[] args) {
        DOMConfigurator.configure("src/main/resources/log4j-config.xml");
        
    }
}
