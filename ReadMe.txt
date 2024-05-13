Note: you may delete files in the directory and run the command to generate those CSV files again to see the results for each command in the

Please DO NOT DELETE THESE FILES: 
EG4-DBDump.xlsx, weekly_qa_report_week1.xlsx, weekly_qa_report_week2.xlsx, weekly_qa_report_week3.xlsx, weekly_qa_report_week4.xlsx


# This command must be run first to generate the CSV files and create the database. 
# Convert xlsx files to csv files then put them to database:
python Project2.py weekly_qa_report_week1.xlsx weekly_qa_report_week2.xlsx weekly_qa_report_week3.xlsx weekly_qa_report_week4.xlsx EG4-DBDump.xlsx

# Get Kevin Chaja data and output a csv containing all data
python Project2.py --user "Kevin Chaja" --output "kevin_chaja_report.csv"

# List all user work done by the individual user (For example Test Owner: "Salman Khan")
python Project2.py --list-all-work --user "Salman Khan"

# List all repeatable bugs export to csv 
python Project2.py --repeatable-bugs --output repeatable_bugs_report.csv

# List all Blockers export to csv
python Project2.py --blocker-bugs --output blocker_bugs_report.csv

# list work done for 3/19/2024 export to csv
python Project2.py --reports-on-date "3/19/2024" --output reports_03192024.csv

# list first case data export to csv
python Project2.py --first-case --output first_test_case.csv

# list middle case data export to csv
python Project2.py --middle-case --output middle_test_case.csv

# list last case data export to csv
python Project2.py --last-case --output last_test_case.csv


