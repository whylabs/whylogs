# Usage Notes
* Extract to a folder on your machine (needs JVM btw)
* You’ll need
  * Input file in CSV format on disk
  * Column name for the date time (required atm)
  * Format of the date time (https://docs.oracle.com/javase/7/docs/api/java/text/SimpleDateFormat.html)
  *You can run with a small limit to test first
* If the date time field is missing, we’ll use UTC epoch 0 time (00:00:00 UTC on 1 January 1970) as the date time for that target)
* Examples
```
Run with first 1000 entries with LendingClub data 
./profiler -i=lendingclub_accepted_2007_to_2017.csv -d=issue_d -f=MMM-yyy -l=1000

Run with NYC ticket dataset (note the quotes)
./profiler -i=Parking_Violations_Issued_-_Fiscal_Year_2017.csv -d="Issue Date" -f=MM/dd/YYYY
```

* Running with TSV dataset
```
profiler-1.0/bin/profiler -i=arcos_all_washpost.tsv/arcos_all_washpost.tsv -d="TRANSACTION_DATE" -f=MMddYYY  -s="\t"
```