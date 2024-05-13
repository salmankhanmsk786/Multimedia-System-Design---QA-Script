import argparse
import pandas as pd
from pathlib import Path
from glob import glob
from pymongo import MongoClient

# Function to convert a given Excel file to a CSV file
def convert_excel_to_csv(excel_file_path, csv_file_path):
    df = pd.read_excel(excel_file_path, index_col=None)
    df.to_csv(csv_file_path, index=False)
    print(f"Converted {excel_file_path} to {csv_file_path}")

def combine_csv_files(files_to_combine, combined_file_name):
    # Read and concatenate all CSV files into one DataFrame
    combined_csv = pd.concat([pd.read_csv(f) for f in files_to_combine])
    # Export the combined data to a new CSV file
    combined_csv.to_csv(combined_file_name, index=False)
    print(f"Combined files into {combined_file_name}")

# Function to connect to MongoDB
def connect_to_mongodb(host='localhost', port=27017):
    client = MongoClient(host, port)
    return client['the_reckoning2']  # create/connect a database called 'the_reckoning2'.

# Function to read a CSV file into a DataFrame
def csv_to_dataframe(csv_file_path):
    return pd.read_csv(csv_file_path)

# Function to insert data from a DataFrame into a MongoDB collection
def insert_data_to_collection(db, collection_name, data_frame):
    collection = db[collection_name]
    records = data_frame.to_dict('records')
    collection.insert_many(records)

# Function to handle exporting a user's data to CSV
def export_user_data_to_csv(db, user_name, csv_file_name):
    user_data = db['collection_2'].find({"Test Owner": {"$regex": user_name, "$options": "i"}})
    user_data_list = list(user_data)

    # Debug: Print the query results
    print(f"Query returned {len(user_data_list)} documents for Test Owner containing '{user_name}'")
    if len(user_data_list) == 0:
        print("No data found for this Test Owner. Exiting without creating CSV.")
        return

    # Convert the data to a DataFrame and export to CSV
    df = pd.DataFrame(user_data_list)
    df.to_csv(csv_file_name, index=False)
    print(f"Data for Test Owner containing '{user_name}' has been exported to {csv_file_name}")

def get_all_work_by_user_combined(db, user_name, csv_file_name):
    pipeline = [
        {"$match": {"Test Owner": {"$regex": f".*{user_name}.*", "$options": "i"}}},
        # Normalize fields
        {"$addFields": {
            "Normalized Test Owner": user_name,
            "Normalized Test Case": {"$trim": {"input": {"$toLower": "$Test Case"}}},
            "Normalized Build": {"$trim": {"input": {"$toLower": "$Build #"}}},
        }},
        {"$unionWith": {
            "coll": "collection_1",
            "pipeline": [
                {"$match": {"Test Owner": {"$regex": f".*{user_name}.*", "$options": "i"}}},
                {"$addFields": {
                    "Normalized Test Owner": user_name,
                    "Normalized Test Case": {"$trim": {"input": {"$toLower": "$Test Case"}}},
                    "Normalized Build": {"$trim": {"input": {"$toLower": "$Build #"}}},
                }}
            ]
        }},
        # Group by normalized fields to eliminate duplicates
        {"$group": {
            "_id": {
                "Build": "$Build #",
                "Category": "$Category",
                "NormalizedTestCase": "$Normalized Test Case",
                "NormalizedBuild": "$Normalized Build",
            },
            "uniqueDocs": {"$addToSet": "$$ROOT"}
        }},
        {"$unwind": "$uniqueDocs"},
        {"$replaceRoot": {"newRoot": "$uniqueDocs"}},
        # Exclude the normalization fields from the final output
        {"$project": {"Normalized Test Owner": 0, "Normalized Test Case": 0}}
    ]
    
    combined_data = list(db['collection_2'].aggregate(pipeline))
    
    # Convert to DataFrame and save as CSV
    df = pd.DataFrame(combined_data)
    df.to_csv(csv_file_name, index=False)
    print(f"All work for {user_name} from both collections has been exported to {csv_file_name}")

def append_unique_rows_to_csv(db, user_name, csv_file_path):
    # Define a set of columns that uniquely identify a record
    unique_columns = ['Build #', 'Category', 'Test Case', 'Test Owner']

    # Attempt to read the existing CSV into a DataFrame or create an empty one if the CSV doesn't exist
    try:
        existing_df = pd.read_csv(csv_file_path)
    except FileNotFoundError:
        existing_df = pd.DataFrame(columns=unique_columns)

    # Query the database for new records
    new_records_cursor = db['collection_2'].find({"Test Owner": {"$regex": user_name, "$options": "i"}})

    # Convert the cursor to a DataFrame
    new_records_df = pd.DataFrame(list(new_records_cursor))

    if new_records_df.empty:
        print("No new records found for the user.")
        return

    new_records_df['Normalized Test Owner'] = new_records_df['Test Owner'].str.lower().str.strip()
    new_records_df['Normalized Test Case'] = new_records_df['Test Case'].str.lower().str.strip()
    new_records_df['Normalized Build'] = new_records_df['Build #'].str.lower().str.strip()

    if existing_df.empty:
        unique_df = new_records_df.drop_duplicates(subset=unique_columns)
        unique_df.to_csv(csv_file_path, index=False)
        return
    
    # Combine the existing and new records and drop duplicates
    combined_df = pd.concat([existing_df, new_records_df]).drop_duplicates(subset=unique_columns)

    # Write the updated DataFrame back to the CSV
    combined_df.to_csv(csv_file_path, index=False)
    print(f"Updated records have been saved to {csv_file_path}")

def list_repeatable_bugs(db, csv_file_path):
    pipeline = [
        {"$match": {"Repeatable?": {"$regex": "^yes$", "$options": "i"}}},
        {"$addFields": {
            "Normalized Repeatable": {"$toLower": "$Repeatable?"}
        }},
        {"$unionWith": {
            "coll": "collection_2", 
            "pipeline": [{"$match": {"Repeatable?": {"$regex": "^yes$", "$options": "i"}}}]
        }},
        {"$group": {
            "_id": {
                "Test": "$Test #",
                "Build #": "$Build #",
                "Category": "$Category",
                "Test Case": "$Test Case",
                "Test Owner": "$Test Owner"

            },
            "uniqueDocs": {"$first": "$$ROOT"}
        }},
        {"$unwind": "$uniqueDocs"},
        {"$replaceRoot": {"newRoot": "$uniqueDocs"}},
        {"$project": {
            "_id": 0 , # Exclude the _id field in the final output
            "Normalized Repeatable": 0
        }}
    ]
    
    repeatable_bugs = list(db['collection_2'].aggregate(pipeline))

    # The count of repeatable bugs
    repeatable_bugs_count = len(repeatable_bugs)
    print(f"Count of repeatable bugs: {repeatable_bugs_count}")

    if repeatable_bugs_count > 0:
        # Convert to DataFrame and save as CSV
        df = pd.DataFrame(repeatable_bugs)
        df.to_csv(csv_file_path, index=False)
        print(f"Repeatable bugs have been exported to {csv_file_path}")
    else:
        print("No repeatable bugs to export.")

def list_blocker_bugs(db, csv_file_path):
    pipeline = [
        {"$match": {"Blocker?": {"$regex": "^yes$", "$options": "i"}}}, 
        {"$unionWith": {
            "coll": "collection_1",
            "pipeline": [{"$match": {"Blocker?": {"$regex": "^yes$", "$options": "i"}}}]
        }},
        {"$group": {
            "_id": {
                "Build #": "$Build #",
                "Category": "$Category",
                "Test Case": "$Test Case",
                "Test Owner": "$Test Owner"
            },
            "uniqueDocs": {"$first": "$$ROOT"}
        }},
        {"$unwind": "$uniqueDocs"},
        {"$replaceRoot": {"newRoot": "$uniqueDocs"}},
        {"$project": {
            "_id": 0  
        }}
    ]
    
    blocker_bugs = list(db['collection_2'].aggregate(pipeline))
    print(f"Found {len(blocker_bugs)} blocker bugs.")

    if blocker_bugs:
        df = pd.DataFrame(blocker_bugs)
        df.to_csv(csv_file_path, index=False)
        print(f"Blocker bugs have been exported to {csv_file_path}")
    else:
        print("No blocker bugs to export.")

def list_reports_on_date(db, date_str, csv_file_path):
    target_date = pd.to_datetime(date_str).strftime('%Y-%m-%d 00:00:00')

    pipeline = [
        # Match documents based on the build date
        {"$match": {"Build #": target_date}},
        {"$unionWith": {
            "coll": "collection_1",
            "pipeline": [{"$match": {"Build #": target_date}}]
        }},
        # Group by a set of fields that uniquely identify a report
        {"$group": {
            "_id": {
                "Build": "$Build #",
                "Category": "$Category",
                "TestCase": "$Test Case",
            },
            "uniqueDocs": {"$first": "$$ROOT"}
        }},
        {"$unwind": "$uniqueDocs"},
        {"$replaceRoot": {"newRoot": "$uniqueDocs"}},
        {"$project": {
            "_id": 0  
        }}
    ]
    
    reports = list(db['collection_2'].aggregate(pipeline))

    print(f"Found {len(reports)} reports for build date {date_str}.")

    if reports:
        df = pd.DataFrame(reports)
        df.to_csv(csv_file_path, index=False)
        print(f"Reports for build date {date_str} have been exported to {csv_file_path}")
    else:
        print(f"No reports to export for build date {date_str}.")


def export_test_case_by_position(db, position, csv_file_path):
    # Count total documents to find the index for the middle document
    total_docs = db['collection_2'].count_documents({})
    middle_index = total_docs // 2

    # Determine the skip amount based on the position requested
    skip_amount = 0 if position == 'first' else middle_index if position == 'middle' else total_docs - 1

    # Pipeline to skip to the desired document and then limit to get just that one document
    pipeline = [
        {"$sort": {"_id": 1}},  # Sort by the _id field, which is inherently ordered by insertion time
        {"$skip": skip_amount},  # Skip to the desired document based on position
        {"$limit": 1}
    ]
    
    # Run the aggregation pipeline
    test_case = list(db['collection_2'].aggregate(pipeline))

    # Check if we got a result
    if test_case:
        # Convert to DataFrame and save as CSV
        df = pd.DataFrame(test_case)
        df.to_csv(csv_file_path, index=False)
        print(f"The {position} test case has been exported to {csv_file_path}")
    else:
        print(f"No {position} test case found in the collection.")





# Set up argparse to accept file paths
def setup_argparse():
    parser = argparse.ArgumentParser(description="Script to convert Excel files to CSV, combine them, insert data into MongoDB, and export data for a specific user.")
    parser = argparse.ArgumentParser(description="Script to manage QA report data.")
    parser.add_argument('--convert', nargs='+', help="Excel files to be converted to CSV.")
    parser.add_argument('--combine', action='store_true', help="Combine all weekly report CSV files into one.")
    parser.add_argument('--user', help="Username to query and export data for.")
    parser.add_argument('--output', help="Output CSV file name.")
    parser.add_argument('--list-all-work', action='store_true',
                        help="List all work done by the user specified by --user from both collections without duplicates.")
    parser.add_argument('--repeatable-bugs', action='store_true',
                        help="List all repeatable bugs from both collections without duplicates.")
    parser.add_argument('--blocker-bugs', action='store_true', help="List all Blocker bugs from both collections without duplicates.")
    parser.add_argument('--reports-on-date', help="List all reports from both collections for a given date without duplicates.")
    parser.add_argument('--first-case', action='store_true', help="Report the first test case from collection 2.")
    parser.add_argument('--middle-case', action='store_true', help="Report the middle test case from collection 2.")
    parser.add_argument('--last-case', action='store_true', help="Report the last test case from collection 2.")  
    return parser.parse_args()


def main():
     # Connect to MongoDB
    db = connect_to_mongodb()
    args = setup_argparse()

    # Process each file provided as argument
    # Convert Excel files if the --convert argument is used
    if args.convert:
        for file_path in args.convert:
            path = Path(file_path)
            if path.is_file() and path.suffix == '.xlsx':
                csv_path = path.with_suffix('.csv')
                convert_excel_to_csv(path, csv_path)
            else:
                print(f"Skipping {file_path}: Not an .xlsx file or does not exist.")

    # Combine CSV files if the --combine argument is used
    if args.combine:
        weekly_report_files = glob('weekly_qa_report_week*.csv')
        combine_csv_files(weekly_report_files, 'combined_weekly_reports.csv')

        # Insert combined data into MongoDB Collection 1
        combined_data_df = csv_to_dataframe('combined_weekly_reports.csv')
        insert_data_to_collection(db, 'collection_1', combined_data_df)

        # Insert EG4-DBDump data into MongoDB Collection 2
        eg4_db_dump_df = csv_to_dataframe('EG4-DBDump.csv')
        insert_data_to_collection(db, 'collection_2', eg4_db_dump_df)

    # Export data for a specific user if the --user and --output arguments are used
    if args.user and args.output:
        export_user_data_to_csv(db, args.user, args.output)

    # List all work done by the user specified by --user from both collections without duplicates
    if args.list_all_work and args.user:
        combined_csv_filename = f"{args.user}_combined_work.csv"
        append_unique_rows_to_csv(db, args.user, combined_csv_filename)

    #  logic for handling --repeatable-bugs argument
    if args.repeatable_bugs:
        if not args.output:
            print("Please specify an output file name using --output")
            return
        list_repeatable_bugs(db, args.output)

    # Handle --blocker-bugs argument
    if args.blocker_bugs:
        if not args.output:
            print("Please specify an output file name using --output")
            return
        list_blocker_bugs(db, args.output)

    # Handle --reports-on-date argument
    if args.reports_on_date:
        if not args.output:
            print("Please specify an output file name using --output")
            return
        list_reports_on_date(db, args.reports_on_date, args.output)

    # Handle the command-line arguments for first, middle, and last cases
    # Determine output filename
    csv_file_name = args.output if args.output else "test_case.csv"
    if args.first_case:
        export_test_case_by_position(db, 'first', csv_file_name)
    elif args.middle_case:
        export_test_case_by_position(db, 'middle', csv_file_name)
    elif args.last_case:
        export_test_case_by_position(db, 'last', csv_file_name)

    
if __name__ == "__main__":
    main()