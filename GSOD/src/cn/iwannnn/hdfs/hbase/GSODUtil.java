package cn.iwannnn.hdfs.hbase;

import org.apache.hadoop.hbase.util.Bytes;

public class GSODUtil {

	// 表名
	public static final String TABLE_NAME = "gsod";
	// 列族
	public static final byte[][] COLUMN_FAMILIES = { Bytes.toBytes("INFO"), Bytes.toBytes("DATA") };
	// INFO
	public static final byte[] INFO_STN = Bytes.toBytes("STN");
	public static final byte[] INFO_WBAN = Bytes.toBytes("WBAN");
	public static final byte[] INFO_DATE = Bytes.toBytes("DATA");
	// DATA
	public static final byte[] DATA_TEMP = Bytes.toBytes("TEMP");
	public static final byte[] DATA_DEWP = Bytes.toBytes("DEWP");
	public static final byte[] DATA_SLP = Bytes.toBytes("SLP");
	public static final byte[] DATA_STP = Bytes.toBytes("STP");
	public static final byte[] DATA_VISIB = Bytes.toBytes("VISIB");
	public static final byte[] DATA_WDSP = Bytes.toBytes("WDSP");
	public static final byte[] DATA_MXSPD = Bytes.toBytes("MXSPD");
	public static final byte[] DATA_GUST = Bytes.toBytes("GUST");
	public static final byte[] DATA_MAX = Bytes.toBytes("MAX");
	public static final byte[] DATA_MIN = Bytes.toBytes("MIN");
	public static final byte[] DATA_PRCP = Bytes.toBytes("PRCP");
	public static final byte[] DATA_SNDP = Bytes.toBytes("SNDP");
	// FRSHTT
	public static final byte[] DATA_FOG = Bytes.toBytes("FOG");
	public static final byte[] DATA_ROD = Bytes.toBytes("ROD");
	public static final byte[] DATA_SOIP = Bytes.toBytes("SOIP");
	public static final byte[] DATA_HAIL = Bytes.toBytes("HAIL");
	public static final byte[] DATA_THUNDER = Bytes.toBytes("THUNDER");
	public static final byte[] DATA_TORCF = Bytes.toBytes("TORCF");

	public static final byte[][] COLUMNS = { INFO_STN, INFO_WBAN, INFO_DATE, DATA_TEMP, DATA_DEWP, DATA_SLP, DATA_STP,
			DATA_VISIB, DATA_WDSP, DATA_MXSPD, DATA_GUST, DATA_MAX, DATA_MIN, DATA_PRCP, DATA_SNDP, DATA_FOG, DATA_ROD,
			DATA_SOIP, DATA_HAIL, DATA_THUNDER, DATA_TORCF };
}
