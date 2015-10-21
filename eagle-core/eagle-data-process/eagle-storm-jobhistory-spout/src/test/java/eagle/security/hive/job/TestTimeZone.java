package eagle.security.hive.job;

import org.junit.Test;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

public class TestTimeZone {
	@Test
	public void testTimeZone(){
		TimeZone utc = TimeZone.getTimeZone("UTC");
		GregorianCalendar calForUTC = new GregorianCalendar(utc);
		calForUTC.set(2015, 7, 8, 0, 0, 0);
		
		TimeZone pdt = TimeZone.getTimeZone("PST");
		GregorianCalendar calForPDT = new GregorianCalendar(pdt);
		calForPDT.set(2015, 7, 8, 0, 0, 0);
		System.out.println((calForUTC.getTimeInMillis()-calForPDT.getTimeInMillis())/1000/60/60);
		
		calForUTC = new GregorianCalendar(pdt);
		System.out.println(calForUTC.get(Calendar.DAY_OF_MONTH));
	}
}
