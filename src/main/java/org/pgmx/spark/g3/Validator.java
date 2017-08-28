package org.pgmx.spark.g3;

import org.pgmx.spark.common.utils.AirConstants;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class Validator implements Serializable {

    private Date leg1Date;
    private Date leg2Date;
    SimpleDateFormat sdf = new SimpleDateFormat(AirConstants.FLT_DATE_PROP_FORMAT);

    private static final String TIME_FORMAT = "HHmm";

    public Validator(String leg1Date) {
        init(leg1Date);
    }

    private void init(String depDate) {
        Calendar cal = Calendar.getInstance();

        try {
            leg1Date = sdf.parse(depDate);
            cal.setTime(leg1Date);
            cal.setFirstDayOfWeek(Calendar.MONDAY); //FIXME check this

            cal.add(Calendar.DAY_OF_WEEK, 2);
            leg2Date = cal.getTime(); // Estimated leg2 date

        } catch (ParseException pe) {
            pe.printStackTrace();
        }
    }


    public String getFormattedLeg1Date() {
        SimpleDateFormat inputSDF = new SimpleDateFormat(AirConstants.FLT_DATE_INPUT_FORMAT);
        return inputSDF.format(leg1Date);
    }

    public boolean isValidLegTime(String timeStr, boolean isBefore12) {
        SimpleDateFormat depDateFormat = new SimpleDateFormat(AirConstants.FLT_DATE_PROP_FORMAT
                + TIME_FORMAT);
        Calendar cal1 = Calendar.getInstance();

        try {
            String depDateWithTime = sdf.format(leg1Date) + timeStr;
            Date depDate = depDateFormat.parse(depDateWithTime);
            cal1.setTime(depDate);

            Calendar allowedCal = (Calendar) cal1.clone();
            allowedCal.set(Calendar.HOUR_OF_DAY, 12);
            allowedCal.set(Calendar.MINUTE, 0);

            if (isBefore12) {
                return cal1.before(allowedCal);
            }
            return cal1.after(allowedCal);
        } catch (ParseException pe) {
            //pe.printStackTrace();
            return false;
        }
    }

    /**
     * Before 12 PM on the given date
     *
     * @param timeStr
     * @return
     */
    public boolean isValidLeg1Time(String timeStr) {
        return isValidLegTime(timeStr, true);
    }

    /**
     * After 12 PM on the given date
     *
     * @param timeStr
     * @return
     */
    public boolean isValidLeg2Time(String timeStr) {
        return isValidLegTime(timeStr, false);
    }

    private boolean isValidLegDate(String flightDateStr, boolean isLeg1) {
        SimpleDateFormat inputSDF = new SimpleDateFormat(AirConstants.FLT_DATE_INPUT_FORMAT);
        try {
            Date fltDate = inputSDF.parse(flightDateStr);
            Calendar cal = Calendar.getInstance();
            cal.setTime(fltDate);

            if (cal.get(Calendar.YEAR) != 2008) {
                return false;
            }
            if (isLeg1) {
                return fltDate.equals(leg1Date);
            }
            return fltDate.equals(leg2Date);
        } catch (ParseException e) {
            return false;
        }
    }

    /**
     * 2 days after L1 date
     *
     * @param flightDateStr
     * @return
     */
    public boolean isValidLeg2Date(String flightDateStr) {
        return isValidLegDate(flightDateStr, false);
    }

    private Date getLeg2Date() {
        return leg2Date;
    }

    private Date getLeg1Date() {
        return leg1Date;
    }

    /**
     * 2008
     *
     * @param flightDateStr
     * @return
     */
    public boolean isValidLeg1Date(String flightDateStr) {
        return isValidLegDate(flightDateStr, true);
    }


    public static void main(String s[]) throws Exception {
        String inpDate = "17/01/2008";
        String wrongL1Date = "02/04/2009";
        String rightL1Date = "2008-01-17";
        String rightL1Time = "1159";
        String wrongL1Time = "1530";
        String wrongL2Date = "18/01/2008";
        String rightL2Date = "2008-01-19";
        String wrongL2Time = "1159";
        String rightL2Time = "1359";

        Validator v = new Validator(inpDate);
        System.out.println("Leg1 date calculated ---> " + v.getLeg1Date());
        System.out.println("Leg2 date calculated ---> " + v.getLeg2Date());

        System.out.println("1 NO ---> " + v.isValidLeg1Date(wrongL1Date));
        System.out.println("2 Yes ---> " + v.isValidLeg1Date(rightL1Date));
        System.out.println("3 No ---> " + v.isValidLeg2Date(wrongL2Date));
        System.out.println("4 Yes ---> " + v.isValidLeg2Date(rightL2Date));
        System.out.println("5 No ---> " + v.isValidLeg1Time(wrongL1Time));
        System.out.println("6 Yes ---> " + v.isValidLeg1Time(rightL1Time));
        System.out.println("7 No ---> " + v.isValidLeg2Time(wrongL2Time));
        System.out.println("8 Yes ---> " + v.isValidLeg2Time(rightL2Time));


        //System.out.println("isValidLeg2Time " + isValidLeg2Time("1159", "2008-12-01"));
        //System.out.println("isValidLeg2Time " + isValidLeg2Time("1201", "2008-12-01"));
        //AirportAvgDriver.validateInputs(new String[]{"a", "b", "c", "d", "e", "04/03/2008"});

    }


}
