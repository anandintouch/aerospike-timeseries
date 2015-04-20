 Sample Application to handle Timeseries data to update and search Product prices
===================================================================================
This application is used for two purpose:
 1. Write/Update Product prices for a given day for a given ProductId.
 
 2. Search/Read  the product prices for a given ProductId and for last  ’n’ number of days.

Data Model/Schema design:

-Time based data is ingested based on number of days/months/year to be tracked ,lets say time_counter(N).

-In this below example, time_counter is 7 ,hence 7 Bins are created to update the price for 7 days and search for the last 7 days or less.

-Some extra Bins are there to store other metadata like “creation date of product”,”ProductId”,”Last bin updated”,”Window begin date” and “last update date” of the product.

+--------------+------+------+------+-------+-------+-------+----------------+--------------+-----------+----------------+------+
| Creation_Dt  | Bin6 | Bin5 | Bin4 | Bin3  | Bin2  | Bin1  | LastBinUpdated | Win_Begin_Dt | ProductId | Last_Update_Dt | Bin7 |
+--------------+------+------+------+-------+-------+-------+----------------+--------------+-----------+----------------+------+
| "03/01/2015" | "$0" | "$0" | "$0" | "$30" | "$10" | "$10" | "Bin3"         | "03/01/2015" | "P1"      | "03/03/2015"   | "$0" |
| "03/01/2015" | "$0" | "$0" | "$0" | "$0"  | "$0"  | "$0"  | "Bin2"         | "03/01/2015" | "P2"      | "03/01/2015"   | "$0" |
+--------------+------+------+------+-------+-------+-------+----------------+--------------+-----------+----------------+------+

1. Write/Update Product prices for a given day for a given ProductId.

- Update the product price for a given product Id for a given date by calling API as below

   public void readAndUpdatePriceData(String productId,Date currentDate,String priceData)
    
- First check the currentDate is after the window begin date to update the price

- For example if the date is starting at "03/01/2015" then price will be changed for Bin1 and “LastBinUpdated” would become Bin1 and “Last_Update_Dt” would be "03/01/2015"

- If the price update comes for "03/03/2015" with price as “$30” then in that case ,it checks if there is any holes(no price update on previous days), in that scenario it would copy the price of the previous(last updated bin) which is $10 and replenish the Bin which was skipped i.e. 2nd days price in Bin2 with the same $10 value.
Then it will update the price data for the 3rd day i.e. in Bin3 .

-  Similarly it will go on until the time_counter reaches to the counter i.e. N=7.

 2. Search/Read  the product prices for a given ProductId and for last  ’n’ number of days.

- Search for the price data for the given last number of days by calling below API

 public Record searchRecord(String prodId,Date currentDate,int lastNumberOfDays)

- If the lastNumberOfDays are greater than time_counter(N) then return "No data found "  else get the price data for given lastNumberOfDays
For example - to search P1 on "03/03/2015" and lastnumberOfDays is 3, then get the price for last 3 days/bins will be as below
 
 Key-Bin3 Value-$30
 Key-Bin2 Value-$10
 Key-Bin1 Value-$10
