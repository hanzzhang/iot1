package pnpiot.serializationperf;

public class LocationJson {
	private long timeStamp;
	private short fixType; // to replace c# byte type which is unsigned
	private int latitude;
	private int longitude;
	private int heading;
	private int altitude;
	private short speed; // to replace c# byte type which is unsigned

	public long getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(long timeStamp) {
		this.timeStamp = timeStamp;
	}

	public short getFixType() {
		return fixType;
	}

	public void setFixType(short fixType) {
		// we are using short to represent c# byte which is unsigned type 0-255
		// we can not use the char type since it will be serialized as a character instead of a number
		if (fixType > 255 || fixType < 0) {
			throw new IllegalArgumentException("Must be between 0-255");
		}
		this.fixType = fixType;
	}

	public int getLatitude() {
		return latitude;
	}

	public void setLatitude(int latitude) {
		this.latitude = latitude;
	}

	public int getLongitude() {
		return longitude;
	}

	public void setLongitude(int longitude) {
		this.longitude = longitude;
	}

	public int getHeading() {
		return heading;
	}

	public void setHeading(int heading) {
		this.heading = heading;
	}

	public int getAltitude() {
		return altitude;
	}

	public void setAltitude(int altitude) {
		this.altitude = altitude;
	}

	public short getSpeed() {
		return speed;
	}

	// we are using short to represent c# byte which is unsigned type 0-255
	// we can not use the char type since it will be serialized as a character instead of a number
	public void setSpeed(short speed) {
		if (speed > 255 || speed < 0) {
			throw new IllegalArgumentException("Must be between 0-255");
		}
		this.speed = speed;
	}
}
