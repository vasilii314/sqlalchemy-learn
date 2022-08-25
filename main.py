from sqlalchemy import create_engine, text

engine = create_engine('connection string', future=True, echo=True)
with engine.connect() as connection:
    # text query
    stmt = text('select emp_id, emp_name from employee where emp_id=:emp_id')
    result = connection.execute(stmt, {'emp_id': 2})  # result object is similar to SQL cursor with more methods
    row = result.first()   # iterable sqlalchemy.engine.row.Row and closes the cursor

    stmt = text('select * from employee')
    result = connection.execute(stmt)
    for emp_id, emp_name in result:
        print(f'employee id: {emp_id}   employee name: {emp_name}')


# SQLAlchemy has no concept of autocommit.
# All commits are added as you go, however the BEGIN statement is added automatically
with engine.connect() as connection:
    connection.execute(
        text('insert into employee_of_month (emp_name) values (:emp_name)'),
        {'emp_name': 'sandy'}
    )
    # The end of the block commits the transaction. Rolls back if there's an exception
    # connection.commit()


# Transactions support "nesting" implemented using the SAVEPOINT
with engine.connect() as connection:
    with connection.begin():
        savepoint = connection.begin_nested()
        connection.execute(
            text('update employee_of_month set emp_name=:emp_name'),
            {'emp_name': 'Patrick'}
        )
        savepoint.rollback()
        with connection.begin_nested():
            connection.execute(
                text('update employee_of_month set emp_name=:emp_name'),
                {'emp_name': 'spongebob'}
            )
            # releases savepoint
        # commits transaction or rollback if exception
    # releases connection to the connection pool

with engine.connect() as connection:
    result = connection.execute(text('select * from employee_of_month')).all()
    for emp_id, emp_name in result:
        print(f'{emp_id}) Employee of month: {emp_name}')

