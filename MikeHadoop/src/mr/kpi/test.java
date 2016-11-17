package mr.kpi;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class test {
	public static void main(String[] args) {
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.CHINA);
//		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = new Date();
		System.out.println("date: " + date);
		String date2 = sdf.format(date);
		System.out.println("date2: " + date2);
	}

}
