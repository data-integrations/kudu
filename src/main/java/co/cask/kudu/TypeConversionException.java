package co.cask.kudu;

/**
 * Exception thrown when there is issue with type conversion from CDAP pipeline schema to Kudu.
 */
public class TypeConversionException extends Exception {
  public TypeConversionException(String s) {
    super(s);
  }
}
