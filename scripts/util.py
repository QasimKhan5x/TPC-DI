from sqlalchemy import text


def execute_batch_validation(engine):
    filepath = r"validation\tpcdi_validation.sql"
    # Read the SQL file
    with open(filepath, "r") as file:
        sql_file = file.read()

    # Execute the SQL commands
    with engine.connect() as connection:
        for query in sql_file.split(";"):
            query = query.strip() + ";"
            if len(query) < 5:
                continue
            connection.execute(text(query))
        connection.commit()
