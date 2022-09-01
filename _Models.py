"""

_Models class.
Creates table instances for the database (PostgreSql)
Object Relational Mapping (ORM) has been used.

AUTHOR : Sencer YÜCEL
Written in 2022 JUNE

"""


from playhouse.postgres_ext import *

# Connection to the Postgresql database.
try:
    db = PostgresqlDatabase('Interpol', host='localhost', port=5432, user='postgres', password='databasePassword')
except Exception as Error:
    db = None
    print("Database connection was not successful: ", Error)


# Class for Meta, all other Models will inherit BaseModel to not have duplicate Meta classes.
class BaseModel(Model):
    class Meta:
        database = db


# Criminals table
class Criminals(BaseModel):
    entity_id = CharField(max_length=14, unique=True)
    name = CharField(max_length=40)
    surname = CharField(max_length=40)
    sex_id = CharField(max_length=1)
    eyes_colors = CharField(max_length=8, null=True)
    hairs_id = CharField(max_length=8, null=True)
    date_of_birth = DateField(null=True)
    weight = IntegerField(null=True)
    height = DecimalField(null=True, decimal_places=2, max_digits=3, auto_round=False)
    photo_num = SmallIntegerField()
    distinguishing_marks = TextField(null=True)
    CREATED_AT = DateTimeField()
    UPDATED_AT = DateTimeField(null=True)
    DELETED_AT = DateTimeField(null=True)


# Languages table
class Languages(BaseModel):
    criminal_id = ForeignKeyField(Criminals, to_field='id')
    language = CharField(max_length=8, null=True)
    CREATED_AT = DateTimeField()
    UPDATED_AT = DateTimeField(null=True)
    DELETED_AT = DateTimeField(null=True)


# Arrest_Warrants table
class Arrest_Warrants(BaseModel):
    criminal_id = ForeignKeyField(Criminals, to_field='id')
    issuing_country = CharField(max_length=3)
    charge = TextField()
    CREATED_AT = DateTimeField()
    UPDATED_AT = DateTimeField(null=True)
    DELETED_AT = DateTimeField(null=True)


# Nationalities table
class Nationalities(BaseModel):
    criminal_id = ForeignKeyField(Criminals, to_field='id')
    nationality = CharField(max_length=8, null=True)
    CREATED_AT = DateTimeField()
    UPDATED_AT = DateTimeField(null=True)
    DELETED_AT = DateTimeField(null=True)


# Photos table
class Photos(BaseModel):
    criminal_id = ForeignKeyField(Criminals, to_field='id')
    photo_url = TextField()
    photo = BlobField()
    CREATED_AT = DateTimeField()
    UPDATED_AT = DateTimeField(null=True)
    DELETED_AT = DateTimeField(null=True)