import snowflake.connector  # Connects your Python code to Snowflake.
from config import SNOWFLAKE_CONFIG  # Correct import


# Function to execute an SQL script
def execute_sql_file(file_path):
    # Step 1: Establish a connection to Snowflake
    connection = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = connection.cursor()  # Create a cursor to execute SQL commands.

    # Step 2: Read the SQL file
    with open(file_path, 'r') as sql_file:  # Open the SQL file in read mode.
        sql_script = sql_file.read()       # Read the entire content of the file.

        # Step 3: Split the file into individual SQL statements and execute them
        for statement in sql_script.split(';'):  # Split by `;` (SQL command separator).
            if statement.strip():  # Skip empty statements (caused by extra semicolons).
                cursor.execute(statement)  # Execute the SQL statement.

    print(f"Executed {file_path}")  # Confirmation message.
    cursor.close()  # Close the cursor.
    connection.close()  # Close the Snowflake connection.

# Deploy scripts
if __name__ == "__main__":
    execute_sql_file('sql/create_objects.sql')       # Execute the script to create objects.
    execute_sql_file('sql/insert_sample_data.sql')   # Execute the script to insert sample data.

