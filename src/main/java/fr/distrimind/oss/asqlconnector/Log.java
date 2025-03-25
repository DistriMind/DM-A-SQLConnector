package fr.distrimind.oss.asqlconnector;

import java.util.function.Supplier;

import fr.distrimind.oss.flexilogxml.common.FlexiLogXML;
import fr.distrimind.oss.flexilogxml.common.log.DMLogger;
import fr.distrimind.oss.flexilogxml.common.log.Marker;
import fr.distrimind.oss.flexilogxml.common.log.MarkerFactory;

/**
 * @author Jason Mahdjoub
 * @since DM-A-SQLConnector 1.0
 * @version 1.O
 */
public class Log {

	private static final DMLogger logger= FlexiLogXML.getLoggerInstance("DM-A-SQLConnector");
	private static final Marker marker= MarkerFactory.getSingleton().getMarker("DM-A-SQLConnector");

	public static void trace(Supplier<String> message)
	{
		logger.trace(marker, message);
	}

	public static void info(Supplier<String> message)
	{
		logger.info(marker, message);
	}
	public static void info(String message)
	{
		logger.info(marker, message);
	}

	public static void debug(Supplier<String> message)
	{
		logger.debug(marker, message);
	}

	public static void error(Supplier<String> message)
	{
		logger.error(marker, message);
	}

	public static void error(Supplier<String> message, Throwable t)
	{
		logger.error(marker, message, t);
	}
}
