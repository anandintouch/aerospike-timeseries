package com.aerospike.timeseries;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Records {
    public static final Map<?, ?>[] RECORDS = {
        map("ProductId", "P1",
            "Creation_Dt",creationDate(),
            "Win_Begin_Dt",creationDate(),
            "Last_Update_Dt",creationDate(),
            "LastBinUpdated","Bin2",
            "Bin1", getStringValue(),
            "Bin2", getStringValue(),
            "Bin3", getStringValue(),
            "Bin4", getStringValue(),
            "Bin5", getStringValue(),
            "Bin6", getStringValue(),
            "Bin7", getStringValue()),
        map("ProductId", "P2",
            "Creation_Dt",creationDate(),
            "Win_Begin_Dt",creationDate(),
            "Last_Update_Dt",creationDate(),
            "LastBinUpdated","Bin2",
            "Bin1", getStringValue(),
            "Bin2", getStringValue(),
            "Bin3", getStringValue(),
            "Bin4", getStringValue(),
            "Bin5", getStringValue(),
            "Bin6", getStringValue(),
            "Bin7", getStringValue())/*,
        map("ProductId", "P3",
            "Creation_Dt",creationDate(),
            "Win_Begin_Dt",creationDate(),
            "LastBinUpdated",0,
            "Bin1", getStringValue(),
            "Bin2", getStringValue(),
            "Bin3", getStringValue(),
            "Bin4", getStringValue(),
            "Bin5", getStringValue(),
            "Bin6", getStringValue(),
            "Bin7", getStringValue()),
        map("ProductId", "P4",
            "Creation_Dt",creationDate(),
            "Win_Begin_Dt",creationDate(),
            "LastBinUpdated",0,
            "Bin1", getStringValue(),
            "Bin2", getStringValue(),
            "Bin3", getStringValue(),
            "Bin4", getStringValue(),
            "Bin5", getStringValue(),
            "Bin6", getStringValue(),
            "Bin7", getStringValue())*/
    };
    
    private static <T> List<T> list(T... aByeArrValues)
    {
        return Arrays.asList(aByeArrValues);
    }
    
    private static String getStringValue(){
    	return "$0";
    }
    
    private static String creationDate(){
    	SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
    	//Calendar now = Calendar.getInstance();
		Calendar timestamp = new GregorianCalendar(2015, 2, 1); // Jan starts from 0
		Date date = new Date(timestamp.getTimeInMillis()); // your date
    	return sdf.format(date);
	
    }
    
    
    private static Map<?, ?> map(
            Object... aInKeysAndValues)
    {
        Map<Object, Object> lMap = new HashMap<>();
        for (int i = 0; i < aInKeysAndValues.length - 1; i += 2)
        {
            lMap.put(aInKeysAndValues[i], aInKeysAndValues[i + 1]);
        }
        return lMap;
    }

}
