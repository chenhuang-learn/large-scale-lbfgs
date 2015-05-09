package hc.parallel.util;

public class DataFormatException extends RuntimeException {

	private static final long serialVersionUID = 7517696652051673281L;

	private String fileName;
	private int lineNumber;

	public DataFormatException(String fileName, int lineNumber, String message) {
		super(message);
		this.fileName = fileName;
		this.lineNumber = lineNumber;
	}
	
	@Override
	public String toString() {
		return "DataFormatException [fileName=" + fileName + ", lineNumber="
				+ lineNumber + ", message=" + getMessage() + "]";
	}

	public static void main(String[] args) {
		DataFormatException de = new DataFormatException("f", 3, "fefe");
		System.out.println(de);
	}

}
