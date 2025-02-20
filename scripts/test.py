import snowflake.connector  # Connects your Python code to Snowflake.
from config import SNOWFLAKE_CONFIG 


# Function to execute test queries
def execute_test_queries(file_path):
    # Step 1: Establish a connection to Snowflake
    connection = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = connection.cursor()  # Create a cursor to execute SQL commands.

    # Step 2: Read the SQL file
    with open(file_path, 'r') as sql_file:  # Open the SQL file in read mode.
        sql_script = sql_file.read()       # Read the entire content of the file.

        # Step 3: Split the file into individual SQL statements and execute them
        for statement in sql_script.split(';'):  # Split by `;` (SQL command separator).
            if statement.strip():  # Skip empty statements.
                cursor.execute(statement)  # Execute the SQL statement.
                print(f"Test Query: {statement.strip()}")  # Print the query.
                print(f"Result: {cursor.fetchall()}")  # Print the result.

    cursor.close()  # Close the cursor.
    connection.close()  # Close the Snowflake connection.

# Run test queries
if __name__ == "__main__":
    execute_test_queries('sql/test_queries.sql')  # Run test queries.

