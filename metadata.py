from sqlalchemy import MetaData, Table, Column, Integer, String, select, create_engine, DateTime, Numeric, Enum, \
    ForeignKey, or_
from sqlalchemy import func

engine = create_engine('postgresql://postgres:31415p@localhost:5432/sqlalchemy', future=True)


# The structure of a relational schema is represented using
# MetaData, Table, and other objects
metadata = MetaData()
user_table = Table(
    'user_account',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('username', String(50), nullable=False),
    Column('fullname', String(225))
)

# Get table column
print(user_table.c.username)
print(user_table.c.username.name)
print(user_table.c.username.type)

# Get primary key
print(user_table.primary_key)

# Generate SELECT
print(select(user_table))

fancy_table = Table(
    'fancy',
    metadata,
    Column('key', String(50), primary_key=True),
    Column('timestamp', DateTime),
    Column('amount', Numeric(10, 2)),
    Column('type', Enum('a', 'b', 'c', name='types'))
)

addresses_table = Table(
    'email_address',
    metadata,
    Column('id', Integer, primary_key=True),
    Column('email_address', String(100), nullable=False),
    Column('user_id', ForeignKey('user_account.id'), nullable=False)
)

# Create table using metadata
with engine.begin() as connection:
    metadata.create_all(connection)


# Get metadata from an existing database
metadata2 = MetaData()
with engine.connect() as connection:
    user_reflected = Table('user_account', metadata2, autoload_with=connection)
    print(user_reflected.c)


# Reflect the whole schema
metadata3 = MetaData()
with engine.connect() as connection:
    metadata3.reflect(connection)
    print(metadata3.tables)



# Expression language
insert_stmt = user_table.insert().values(
    username='spongebob', fullname='Spongebob Squarepants'
)

with engine.begin() as connection:
    # connection.execute(insert_stmt)
    results = connection.execute(user_table.select())
    for result in results:
        print(result)


# insert multiple rows
with engine.begin() as connection:
    connection.execute(
        user_table.insert(),
        [
            {'username': 'patrick', 'fullname': 'Patrick Star'},
            {'username': 'squidward', 'fullname': 'Squidward Tentacles'},
        ]
    )
    results = connection.execute(user_table.select()).all()
    for result in results:
        print(result)


# Select specified or all columns
with engine.connect() as connection:
    select_stmt = (
        select(user_table.c.username, user_table.c.fullname)
        .where(user_table.c.username == 'spongebob')
    )
    results = connection.execute(select_stmt)
    for result in results:
        print(result)

    select_stmt = select(user_table).where(
        or_(
            user_table.c.username == 'spongebob',
            user_table.c.username == 'sandy'
        )
    ).order_by(user_table.c.username)
    results = connection.execute(select_stmt)

    # return exactly one row
    # Throws an error when result set is either empty or contains more than two elements
    print(results.one())

    # Select specific columns and change their order
    results = connection.execute(
        select(user_table).order_by(user_table.c.username)
    )
    for fullname, username in results.columns('fullname', 'username'):
        print(f'{fullname} {username}')

    # Select only one column and return values from it
    results = connection.execute(
        select(user_table).order_by(user_table.c.username)
    )
    for fullname in results.scalars('fullname'):
        print(fullname)

    for result in results:
        print(result)


# Update statement
# Values call is equivalent to SQL SET clause
with engine.begin() as connection:
    update_stmt = (
        user_table.update()
        .values(fullname='Patrick Star')
        .where(user_table.c.username == 'patrick')
    )
    connection.execute(update_stmt)


with engine.begin() as connection:
    connection.execute(
        addresses_table.insert(),
        [
            {'user_id': 1, 'email_address': 'spongebob@spongebob.com'},
            {'user_id': 1, 'email_address': 'spongebob@gmail.com'},
            {'user_id': 4, 'email_address': 'sandy@yahoo.com'},
            {'user_id': 2, 'email_address': 'patrick@gmail.com'}
        ]
    )


# Joins
with engine.connect() as connection:
    stmt = select(user_table.c.username, addresses_table.c.email_address).\
        join(addresses_table, user_table.c.id == addresses_table.c.user_id)
    results = connection.execute(stmt)
    for result in results:
        print(result)


# Subqueries
select_subq = select(user_table.c.username, addresses_table.c.email_address).\
    join(addresses_table, user_table.c.id == addresses_table.c.user_id).\
    subquery()

stmt = select(select_subq.c.username).where(select_subq.c.username == 'spongebob').distinct()
with engine.connect() as connection:
    results = connection.execute(stmt)
    for result in results:
        print(result)

# Aggregate Functions
address_subq = select(
    addresses_table.c.user_id,
    func.count(addresses_table.c.id).label('count')
).group_by(addresses_table.c.user_id).subquery()

username_plus_count = select(user_table.c.username, address_subq.c.count).\
    join(address_subq).order_by(user_table.c.username)

with engine.connect() as connection:
    results = connection.execute(username_plus_count).all()
    for result in results:
        print(result)



