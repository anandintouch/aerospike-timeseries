package com.aerospike.timeseries;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.aerospike.client.Bin;

public class Util {
	
	public static final int DAYS_COUNTER = 7;
	public static final SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
	
    public static Bin[] getBins(Map<String, Object> aInValues)
    {
        List<Bin> lBins = new ArrayList<>();
        for (Map.Entry<String, Object> lEntry : aInValues.entrySet())
        {
            Object lValue = lEntry.getValue();
            Bin lBin;
            if (lValue instanceof List)
            {
                lBin = Bin.asList(lEntry.getKey(), (List<?>) lValue);
            }
            else if (lValue instanceof Map)
            {
                lBin = Bin.asMap(lEntry.getKey(), (Map<?, ?>) lValue);
            }
            else
            {
                lBin = new Bin(lEntry.getKey(), lValue);
            }
            lBins.add(lBin);
        }
        // Convert list to an array and return it
        return lBins.toArray(new Bin[lBins.size()]);
    }
    
    public static List<String> findBinToRetrieveData(Calendar givenDate ){
    	//System.out.println("Todays date -"+new Date());
    	String rightBin = null;
    	List<String> binList = new ArrayList<String>();
    	
    	for (Entry<Long, String> entry : getTimeBinMap().entrySet()) {
    		Calendar mapTime = new GregorianCalendar();
    		mapTime.setTimeInMillis(entry.getKey());

    		if(givenDate.equals(mapTime)){
    			rightBin = entry.getValue();
    			System.out.println("Time matched, BIN is: "+entry.getValue());
    			binList.add(rightBin);
    		}else{
    			//System.out.println("Other BINs in map are: "+entry.getValue());
    		}

		}
    	return binList;
    }

    
    public static List<Bin> findBinToReplenish(Date currentDate,Object lastBinUpdated,String binToBeUpdated,String data){
    	List<Bin> binList = new ArrayList<Bin>();
    	
    	//String binToRefill = null;
    	int lastBinUpdatedNum = getBinNumber(lastBinUpdated);
    	int binToBeUpdatedNum = getBinNumber(binToBeUpdated);
    	
    	for(int i=lastBinUpdatedNum; i<binToBeUpdatedNum;i++){
    		//binToRefill = lastBinUpdated.toString();
    		binList.add(new Bin("Bin"+i,data));
    	}
    	
    	// Also test if lastbin updated is 6th on march 6th and then update on march 9 (bin2), 
    	// 6,7 and 1st bins should be replenished and Bin2 should be updated with new price
    	int counter = 0;
    	if(binToBeUpdatedNum <= lastBinUpdatedNum ){
    		if(Util.getSpecificDay(currentDate) > Util.DAYS_COUNTER && lastBinUpdatedNum == Util.DAYS_COUNTER){
    			int startLoop = binToBeUpdatedNum -1 ;
    			 for(int i=startLoop; ;i--){
      				counter ++;
      				
      				if(i==0){
      					i = Util.DAYS_COUNTER;
      				}
      				binList.add(new Bin("Bin"+i,data));
      				if(counter == calculateBinNum(Util.getSpecificDay(currentDate))){
      	     			break;
      	     		}
      	  		 }
    			
    		}
    		
           /* for(int i=binToBeUpdatedNum;i<lastBinUpdatedNum ;i++){
            	binList.add(new Bin("Bin"+i,data));
          	}*/
        }
    	
		return binList;
    	
    }
    
    public static String findBinToUpdate(Date currentDate){
    	String binName =null;
    	
    	int currDay = getSpecificDay(currentDate);
    	int binToUpdateFormula1 = Math.abs(currDay - DAYS_COUNTER);  // X
    	
    	if (currDay<=DAYS_COUNTER){
    		binName ="Bin"+currDay;
    	}else if(binToUpdateFormula1 <= DAYS_COUNTER){     // check if X > N (days counter for example 7 bins)
    		binName = "Bin"+binToUpdateFormula1;
    	}else {
    		//check if binToUpdateFormula1 > DAYS_COUNTER
    		binName = "Bin"+(binToUpdateFormula1 - DAYS_COUNTER);
    	}

		return binName;
    }
    
    public static int getSpecificDay(Date currentDate){
    	//Calendar timestamp = new GregorianCalendar(2015, 8, 15);
    	int dayOfDate = 0;
    	if(currentDate != null ){
    		Calendar now = Calendar.getInstance();
    		now.setTime(currentDate); 
    		
    		int year = now.get(Calendar.YEAR);
    		int month = now.get(Calendar.MONTH); // Note: Month starts at 0
    		dayOfDate = now.get(Calendar.DAY_OF_MONTH);
    		
    	}
		return dayOfDate;

    }
    
    public static Date calculateNewWindowBeginDate(Date currentDate,Date oldWinBeginDate){
    	int newWinBeginDay = 0;
    	int currentDay =0;
    	int oldWinBeginDay =0;
    	Date newWinBeginDate = null;
    	
    	currentDay = getSpecificDay(currentDate);
    	
    	if(currentDay > DAYS_COUNTER){
        	oldWinBeginDay = getSpecificDay(oldWinBeginDate);
        	
        	int modVal = (currentDay-oldWinBeginDay) % DAYS_COUNTER; 
        	
        	newWinBeginDay = oldWinBeginDay + modVal;
        	System.out.println("newWinBeginDay:"+newWinBeginDay);
        	
        	//SimpleDateFormat sdf = new SimpleDateFormat("MM/dd/yyyy");
        	Calendar c = Calendar.getInstance(); 
        	c.setTime(oldWinBeginDate); 
        	if(modVal == 0){
        		c.add(Calendar.DATE,modVal+1);
        	}
        	c.add(Calendar.DATE,modVal );
        	
        	/*Date date = sdf.parse(dateInString);    // string date to date
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);*/
        	
        	newWinBeginDate = c.getTime();
        	System.out.println("Final new Win_Begin_Date: "+sdf.format(c.getTime()));
    		
    	}else{
    		newWinBeginDate = oldWinBeginDate;
    	}

		return newWinBeginDate;
    	
    }
    
    public static boolean checkIfHoles(Object lastBinUpdated,Date currentDate,String binToBeUpdated){
    	int lastBinNum =0;
    	int binToBeUpdatedNum=0;
    	
    	lastBinNum = getBinNumber(lastBinUpdated);
    	binToBeUpdatedNum = getBinNumber(binToBeUpdated);
    	
    	if(Math.abs(getSpecificDay(currentDate)-lastBinNum) > 1){
    		if(Math.abs(getSpecificDay(currentDate)-lastBinNum) > DAYS_COUNTER && ! (Math.abs(binToBeUpdatedNum-lastBinNum) >1)){
    			return false;
    			
    		}
    		return true;
    	}
		return false;
    	
    }
    
    public static  int getBinNumber(Object binName){
    	int lastBinNum =0;
    	Matcher matcher = Pattern.compile("[0-9]+").matcher(binName.toString()); 
    	while (matcher.find()) { 
    		lastBinNum =Integer.parseInt(matcher.group());
    	}
		return lastBinNum;
    }
    
    public static int calculateBinNum(int givenDay){
    	int num=0; 

    	if(givenDay > Util.DAYS_COUNTER){
    		num = Math.abs(givenDay - Util.DAYS_COUNTER);
    	}
    	
    /*	skipRecursivePaths = true;
    	if(skipRecursivePaths && num < 7 ) {
    		return num;
    	}
    	calculateBinNum(num);*/
		return num;
    }
    
    private static Map<Long,String> getTimeBinMap(){
    	Map<Long,String> timeBinMap = new HashMap<Long,String>();
    	
    	Calendar timestamp1 = new GregorianCalendar(2014, 1, 2);
    	timeBinMap.put(timestamp1.getTimeInMillis(), "Bin1");
    	
		Calendar timestamp2 = new GregorianCalendar(2014, 2,10);	
		timeBinMap.put(timestamp2.getTimeInMillis(),"Bin2");
		
		Calendar timestamp3 = new GregorianCalendar(2014, 3, 30);	
		timeBinMap.put(timestamp3.getTimeInMillis(),"Bin3");
		Calendar timestamp4 = new GregorianCalendar(2014, 4, 30);	
		timeBinMap.put(timestamp4.getTimeInMillis(),"Bin4");
		Calendar timestamp5 = new GregorianCalendar(2014, 5, 30);	
		timeBinMap.put(timestamp5.getTimeInMillis(),"Bin5");
		Calendar timestamp6 = new GregorianCalendar(2014, 6, 30);	
		timeBinMap.put(timestamp6.getTimeInMillis(),"Bin6");
		Calendar timestamp7 = new GregorianCalendar(2014, 7, 30);	
		timeBinMap.put(timestamp7.getTimeInMillis(),"Bin7");
		Calendar timestamp8 = new GregorianCalendar(2014, 8, 30);	
		timeBinMap.put(timestamp8.getTimeInMillis(),"Bin8");
		Calendar timestamp9 = new GregorianCalendar(2014, 9, 30);	
		timeBinMap.put(timestamp9.getTimeInMillis(),"Bin9");
		Calendar timestamp10 = new GregorianCalendar(2014, 10, 30);	
		timeBinMap.put(timestamp10.getTimeInMillis(),"Bin10");
		Calendar timestamp11 = new GregorianCalendar(2014, 11, 30);	
		timeBinMap.put(timestamp11.getTimeInMillis(),"Bin11");
		Calendar timestamp12 = new GregorianCalendar(2014, 12, 31);	
		timeBinMap.put(timestamp12.getTimeInMillis(),"Bin12");
		Calendar timestamp13 = new GregorianCalendar(2015, 1, 1);	
		timeBinMap.put(timestamp13.getTimeInMillis(),"Bin13");
		Calendar timestamp14 = new GregorianCalendar(2015, 2, 2);	
		timeBinMap.put(timestamp14.getTimeInMillis(),"Bin14");
		Calendar timestamp15 = new GregorianCalendar(2015, 3, 3);	
		timeBinMap.put(timestamp15.getTimeInMillis(),"Bin15");
		Calendar timestamp16 = new GregorianCalendar(2015, 4, 30);	
		timeBinMap.put(timestamp16.getTimeInMillis(),"Bin16");
		Calendar timestamp17 = new GregorianCalendar(2015, 5, 30);	
		timeBinMap.put(timestamp17.getTimeInMillis(),"Bin17");
		Calendar timestamp18 = new GregorianCalendar(2015, 6, 30);	
		timeBinMap.put(timestamp18.getTimeInMillis(),"Bin18");
		Calendar timestamp19 = new GregorianCalendar(2015, 7, 30);	
		timeBinMap.put(timestamp19.getTimeInMillis(),"Bin19");
		Calendar timestamp20 = new GregorianCalendar(2015, 8, 30);	
		timeBinMap.put(timestamp20.getTimeInMillis(),"Bin20");
		Calendar timestamp21 = new GregorianCalendar(2015, 9, 30);	
		timeBinMap.put(timestamp21.getTimeInMillis(),"Bin21");
		Calendar timestamp22 = new GregorianCalendar(2015, 10, 30);	
		timeBinMap.put(timestamp22.getTimeInMillis(),"Bin22");
		Calendar timestamp23 = new GregorianCalendar(2015, 11, 30);	
		timeBinMap.put(timestamp23.getTimeInMillis(),"Bin23");
		Calendar timestamp24 = new GregorianCalendar(2015, 12, 31);	
		timeBinMap.put(timestamp24.getTimeInMillis(),"Bin24");
		
		return timeBinMap;
    	
    }

}
