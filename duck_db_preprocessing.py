import duckdb
import glob
import os

def main():
    # Define paths
    archive_dir = './data/archive'
    db_file = './data/combined_tweets'
    
    # Connect (or create) the DuckDB database
    con = duckdb.connect(database=db_file)
    
    # Find all CSV files in the archive directory
    csv_files = glob.glob(os.path.join(archive_dir, '*.csv'))
    if not csv_files:
        print(f"No CSV files found in {archive_dir}")
        return

    first = True  # Flag to create the table for the first file only
    for csv_file in csv_files:
        print(f"Processing file: {csv_file}")
        if first:
            # For the first file, create the table and load its content.
            con.execute(f"""
                CREATE TABLE combined_tweets AS
                SELECT * FROM read_csv_auto('{csv_file}');
            """)
            first = False
        else:
            # For subsequent files, append the data.
            con.execute(f"""
                INSERT INTO combined_tweets
                SELECT * FROM read_csv_auto('{csv_file}');
            """)
    
    print(f"All files have been processed and combined into 'combined_tweets' in the database: {db_file}")
    con.close()

if __name__ == '__main__':
    main()
