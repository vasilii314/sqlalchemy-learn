from sqlalchemy import Column, Integer, String, select, ForeignKey
from sqlalchemy.future import create_engine
from sqlalchemy.orm import registry, sessionmaker, relationship

engine = create_engine('postgresql://postgres:31415p@localhost:5432/sqlalchemy', future=True, echo=True)

mapper_registry = registry()

# Session is required to persist and load User objects from database
# Session object makes use of connection factory (engine) and will handle
# the job of connecting, committing, and releasing connections to this engine
Session = sessionmaker(bind=engine, future=True)


# Constructor automatically generated based on declared fields
@mapper_registry.mapped
class User:
    __tablename__ = 'user_account_orm'

    id = Column(Integer, primary_key=True)
    username = Column(String)
    fullname = Column(String)
    addresses = relationship('Address', back_populates='user')

    def __repr__(self):
        return '<User(%r, %r)' % (self.username, self.fullname)


@mapper_registry.mapped
class Address:
    __tablename__ = 'email_address_orm'

    id = Column(Integer, primary_key=True)
    email_address = Column(String, nullable=False)
    user_id = Column(ForeignKey('user_account.id'), nullable=False)

    user = relationship('User', back_populates='addresses')

    def __repr__(self):
        return '<Address(%r)' % self.email_address


print(User.__table__)
print(select(User))

spongebob = User(username='spongebob', fullname='Spongebob Squarepants')
print(spongebob.id)

# Create database schema
with engine.begin() as connection:
    mapper_registry.metadata.create_all(connection)


session = Session()  # start transaction
session.add(spongebob)

select_stmt = select(User).filter_by(username='spongebob')
result = session.execute(select_stmt)

# Add multiple objects to session
session.add_all(
    [
        User(username='patrick', fullname='Patrick Star'),
        User(username='sandy', fullname='Sandy Cheeks')
    ]
)

# Modify object in a transaction
# The object is now marked 'dirty'
spongebob.fullname = 'Spongebob Jones'

# Commit transaction
session.commit()

