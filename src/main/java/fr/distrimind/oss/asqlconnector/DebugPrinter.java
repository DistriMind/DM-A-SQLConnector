package fr.distrimind.oss.asqlconnector;

/**
 * thanks to
 * <a href="http://blog.taragana.com/index.php/archive/core-java-how-to-get-java-source-code-line-number-file-name-in-code/">...</a>!
 *
 * @author klm
 */
public class DebugPrinter {

	// original tutorial suggests items [2]
	// from arrays, but [3] works for android.
	// there you go..

	/**
	 * Get the current line number.
	 *
	 * @return int - Current line number.
	 */
	public static int getLineNumber() {
		return Thread.currentThread().getStackTrace()[3].getLineNumber();
	}

	public static String getFileName() {
		return Thread.currentThread().getStackTrace()[3].getClassName()
				+ " (" + Thread.currentThread().getStackTrace()[3].getMethodName() + ")";
	}

}
