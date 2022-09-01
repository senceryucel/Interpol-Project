"""

_Database_Operations class.
Extracts data from the website of Interpol.
Manipulate data into correct forms.
Inserts all data to database (PostgreSql).
Updates the tables if tables already exist.

AUTHOR : Sencer YÜCEL
Written in 2022 JUNE

"""

from PIL import Image
from playhouse.postgres_ext import *
from datetime import datetime
import requests
import urllib.request
import _Models

# Connection to the Postgresql database.
try:
    db = PostgresqlDatabase('Interpol', host='localhost', port=5432, user='postgres', password='databasePassword')
except Exception as Error:
    db = None
    print("Database connection was not successful: ", Error)

Languages = _Models.Languages
Arrest_Warrants = _Models.Arrest_Warrants
Nationalities = _Models.Nationalities
Photos = _Models.Photos
Criminals = _Models.Criminals


# Form correctness of eyes_colors and hairs to push to the database since they are not in the correct form (e.g. ['BLA']).
def form_corrector(item):
    if item is not None:
        item = item[0]
    return item


# Drops and creates all tables from scratch.
def recreate_tables():
    MODELS = (Languages, Arrest_Warrants, Nationalities, Photos, Criminals)
    db.drop_tables(MODELS)
    db.create_tables(MODELS)


# Extracts data and creates tables.
def table_creator():
    print("Tables are going to be created...")
    hasElement = True
    url_x = 1
    recreate_tables()

    # Main loop for the program that controlled by the variable hasElement, which is being iterated until TOTAL_PUBLIC_PAGES.
    while hasElement:

        # Creating the very first url. Make it suitable for further pages with "url_x = url_x + 1" line.
        conv_url_x = str(url_x)
        URL = 'https://ws-public.interpol.int/notices/v1/red?page=' + conv_url_x
        url_x = url_x + 1

        # Requesting the URL and then transfer that URL into JSON file.
        resp = requests.get(url=URL)
        data = resp.json()

        RECORDS_IN_A_PAGE = data["query"]["resultPerPage"]
        TOTAL_PUBLIC_PAGES = data["_links"]["last"]["href"][-1]

        # URL for self records.
        SELF_URL = "https://ws-public.interpol.int/notices/v1/red/"
        i = 0
        print()
        print("----- Page Number", conv_url_x, "------")
        while i < RECORDS_IN_A_PAGE:
            # Initial information of the i th criminal.
            surname = (data["_embedded"]["notices"][i]["forename"])
            date_of_birth = (data["_embedded"]["notices"][i]["date_of_birth"])
            entity_id = (data["_embedded"]["notices"][i]["entity_id"])
            name = (data["_embedded"]["notices"][i]["name"])

            # To have a valid DATE type for SQL. If a criminal does not have month/day information of birthdate, we accept it 01/01 as default.
            if "/" not in date_of_birth:
                date_of_birth = date_of_birth + "/01/01"

            # We can not have the character ' in any variable (e.g.: D'Aversa) because of the syntax restrictions of SQL.
            if "'" in name:
                name = name.replace("'", "")

            # Customized URL for self records.
            # This must change in the loop, because self record URLs depend on the entity_id of the criminal.
            resp_self = requests.get(url=(SELF_URL + entity_id.replace("/", "-")))
            data_self = resp_self.json()

            # Self records of the criminal.
            weight = (data_self["weight"])
            height = (data_self["height"])
            sex_id = (data_self["sex_id"])
            distinguishing = (data_self["distinguishing_marks"])
            eyes_colors = (data_self["eyes_colors_id"])
            hairs = (data_self["hairs_id"])

            eyes_colors = form_corrector(eyes_colors)
            hairs = form_corrector(hairs)

            # Image(s).
            # resp_image is -> https://ws-public.interpol.int/notices/v1/red/ENTITY-ID/images
            resp_image = requests.get(url=data_self["_links"]["images"]["href"])
            data_image = resp_image.json()

            # To decide how many photos do we have which belongs to the criminal.
            photoNum = len(data_image["_embedded"]["images"])
            i = i + 1
            # print(i, end=" ")

            try:
                # Checker for insertions to ensure that there does not exist a criminal with an entity id that already occurs.
                user = Criminals.get_or_none(entity_id=entity_id)
                if user is None:
                    # Insertion to criminals table.
                    Criminals.insert(entity_id=entity_id, name=name, surname=surname, sex_id=sex_id,
                                     eyes_colors=eyes_colors, hairs_id=hairs,
                                     date_of_birth=date_of_birth, weight=weight, height=height, photo_num=photoNum,
                                     distinguishing_marks=distinguishing,
                                     CREATED_AT=datetime.now()).execute()

                    # Insertion to languages table.
                    if data_self["languages_spoken_ids"] is not None:
                        for m in range(len((data_self["languages_spoken_ids"]))):
                            Languages.insert(criminal_id=int(str(Criminals.get_or_none(entity_id=entity_id))),
                                             language='{}'.format(data_self["languages_spoken_ids"][m]),
                                             CREATED_AT=datetime.now()).execute()
                    else:
                        Languages.insert(criminal_id=int(str(Criminals.get_or_none(entity_id=entity_id))),
                                         language=None, CREATED_AT=datetime.now()).execute()

                    # Insertion to arrest_warrants table.
                    for j in range(len(data_self["arrest_warrants"])):
                        Arrest_Warrants.insert(criminal_id=int(str(Criminals.get_or_none(entity_id=entity_id))),
                                               issuing_country=data_self["arrest_warrants"][j]["issuing_country_id"],
                                               charge=data_self["arrest_warrants"][j]["charge"].replace("'", ""),
                                               CREATED_AT=datetime.now()).execute()

                    # Insertion to nationalities table.
                    if (data["_embedded"]["notices"][i - 1]["nationalities"]) is not None:
                        for k in range(len((data["_embedded"]["notices"][i - 1]["nationalities"]))):
                            if (data["_embedded"]["notices"][i - 1]["nationalities"][k]) is not None:
                                Nationalities.insert(criminal_id=int(str(Criminals.get_or_none(entity_id=entity_id))),
                                                     nationality=data["_embedded"]["notices"][i - 1]["nationalities"][
                                                         k],
                                                     CREATED_AT=datetime.now()).execute()

                            else:
                                Nationalities.insert(criminal_id=int(str(Criminals.get_or_none(entity_id=entity_id))),
                                                     nationality=None, CREATED_AT=datetime.now()).execute()
                    else:
                        Nationalities.insert(criminal_id=int(str(Criminals.get_or_none(entity_id=entity_id))),
                                             nationality=None, CREATED_AT=datetime.now()).execute()

                    # Insertion to photos table.
                    if photoNum != 0:
                        for n in range(photoNum):
                            photoURL = "https://ws-public.interpol.int/notices/v1/red/{}/images/{}".format(
                                entity_id.replace("/", "-"),
                                data_image["_embedded"]["images"][n]["picture_id"])
                            try:
                                urllib.request.urlretrieve(photoURL, "image")
                                img = Image.open("image").tobytes()
                                Photos.insert(criminal_id=int(str(Criminals.get_or_none(entity_id=entity_id))),
                                              photo_url=photoURL, photo=img,
                                              CREATED_AT=datetime.now()).execute()
                            except Exception as photo_exception:
                                print(photo_exception, "occurred, image could not have inserted.")
            except Exception as error:
                print("ERROR", error.__class__.__qualname__, error)

        if conv_url_x == "{}".format(TOTAL_PUBLIC_PAGES):
            print("Tables have been created successfully.")
            hasElement = False


# Looks into all tables with comparing them with the data which have scraped at that moment.
# If there exists any change, then performs.
def table_updater():
    print("\nTables are being updated...")
    url_x = 1
    hasElement = True
    latest_entity_ids = set()
    records_to_delete = 0

    """
    if __debug__:
        Criminals.insert(entity_id="9999/99999", name="SENCER", surname="YÜCEL", sex_id='M',
                         eyes_colors='BLA', hairs_id='BLA',
                         date_of_birth='08-01-2000', weight=98, height=1.86, photo_num=2,
                         distinguishing_marks=None,
                         CREATED_AT=datetime.now()).execute()
    """

    # Same loop with table_creator method to iterate over given URL.
    while hasElement:
        conv_url_x = str(url_x)
        URL = 'https://ws-public.interpol.int/notices/v1/red?page=' + conv_url_x
        url_x = url_x + 1
        resp = requests.get(url=URL)
        data = resp.json()
        SELF_URL = "https://ws-public.interpol.int/notices/v1/red/"
        RECORDS_IN_A_PAGE = data["query"]["resultPerPage"]
        TOTAL_PUBLIC_PAGES = data["_links"]["last"]["href"][-1]
        print("\n", conv_url_x)

        # Loop to have that moment's records.
        # If there is a record that does not exist in the Criminal Table, we create that record.
        # If there is a record that has been changed, we update that record.
        for i in range(RECORDS_IN_A_PAGE):
            print((i + 1), end=" ")
            entity_id = (data["_embedded"]["notices"][i]["entity_id"])
            latest_entity_ids.add(entity_id)
            resp_self = requests.get(url=(SELF_URL + entity_id.replace("/", "-")))
            data_self = resp_self.json()
            distinguishing = (data_self["distinguishing_marks"])
            hairs = (data_self["hairs_id"])
            hairs = form_corrector(hairs)

            try:
                # If hairs_id has been changed.
                if Criminals.select().where(Criminals.entity_id == entity_id).get().hairs_id != hairs:
                    Criminals.update({Criminals.UPDATED_AT: datetime.now()}).where(
                        Criminals.id == Criminals.get(Criminals.entity_id == entity_id)).execute()
                    Criminals.update({Criminals.hairs_id: hairs}).where(
                        Criminals.id == Criminals.get(Criminals.entity_id == entity_id)).execute()

                # If distinguishing marks have been changed.
                if Criminals.select().where(
                        Criminals.entity_id == entity_id).get().distinguishing_marks != distinguishing:
                    Criminals.update({Criminals.UPDATED_AT: datetime.now()}).where(
                        Criminals.id == Criminals.get(Criminals.entity_id == entity_id)).execute()
                    Criminals.update({Criminals.distinguishing_marks: distinguishing}).where(
                        Criminals.id == Criminals.get(Criminals.entity_id == entity_id)).execute()

                # TODO: Find a way to update all data; Languages, Arrest warrants etc.
                # TODO: There have to be loops, but we also need an index structure to have all data correctly from the database.
                # TODO: Below lines are trials for the task described above.
                for m in range(len((data_self["languages_spoken_ids"]))):
                    print(Languages.select().where(
                        Languages.id == Criminals.get(Criminals.entity_id == entity_id)).get().language)
                # If languages have been changed.
                if Languages.select().where(Languages.criminal_id == Criminals.get(
                        Criminals.entity_id == entity_id)).get().language != hairs:
                    pass




            except Criminals.DoesNotExist:
                records_to_delete = records_to_delete + 1
                print("There is a record to delete. Total number of records to delete is: {}".format(records_to_delete))

            # print(Criminals.select().where(Criminals.entity_id == entity_id).count())
            # If there is a new record
            if Criminals.select().where(Criminals.entity_id == entity_id).count() != 1:
                try:
                    name = (data["_embedded"]["notices"][i]["name"])
                    surname = (data["_embedded"]["notices"][i]["forename"])
                    sex_id = (data_self["sex_id"])
                    eyes_colors = (data_self["eyes_colors_id"])
                    weight = (data_self["weight"])
                    height = (data_self["height"])
                    date_of_birth = (data["_embedded"]["notices"][i]["date_of_birth"])
                    resp_image = requests.get(url=data_self["_links"]["images"]["href"])
                    data_image = resp_image.json()
                    photoNum = len(data_image["_embedded"]["images"])

                    Criminals.insert(entity_id=entity_id, name=name, surname=surname, sex_id=sex_id,
                                     eyes_colors=eyes_colors, hairs_id=hairs,
                                     date_of_birth=date_of_birth, weight=weight, height=height, photo_num=photoNum,
                                     distinguishing_marks=distinguishing,
                                     CREATED_AT=datetime.now()).execute()

                    # Insertion to languages table.
                    if data_self["languages_spoken_ids"] is not None:
                        for m in range(len((data_self["languages_spoken_ids"]))):
                            Languages.insert(criminal_id=int(str(Criminals.get_or_none(entity_id=entity_id))),
                                             language='{}'.format(data_self["languages_spoken_ids"][m]),
                                             CREATED_AT=datetime.now()).execute()
                    else:
                        Languages.insert(criminal_id=int(str(Criminals.get_or_none(entity_id=entity_id))),
                                         language=None, CREATED_AT=datetime.now()).execute()

                    # Insertion to Arrest_Warrants table
                    for j in range(len(data_self["arrest_warrants"])):
                        Arrest_Warrants.insert(criminal_id=int(str(Criminals.get_or_none(entity_id=entity_id))),
                                               issuing_country=data_self["arrest_warrants"][j]["issuing_country_id"],
                                               charge=data_self["arrest_warrants"][j]["charge"].replace("'", ""),
                                               CREATED_AT=datetime.now()).execute()

                    # Insertion to Nationalities table.
                    if (data["_embedded"]["notices"][i - 1]["nationalities"]) is not None:
                        for k in range(len((data["_embedded"]["notices"][i - 1]["nationalities"]))):
                            if (data["_embedded"]["notices"][i - 1]["nationalities"][k]) is not None:
                                Nationalities.insert(criminal_id=int(str(Criminals.get_or_none(entity_id=entity_id))),
                                                     nationality=data["_embedded"]["notices"][i - 1]["nationalities"][
                                                         k],
                                                     CREATED_AT=datetime.now()).execute()

                            else:
                                Nationalities.insert(criminal_id=int(str(Criminals.get_or_none(entity_id=entity_id))),
                                                     nationality=None, CREATED_AT=datetime.now()).execute()
                    else:
                        Nationalities.insert(criminal_id=int(str(Criminals.get_or_none(entity_id=entity_id))),
                                             nationality=None, CREATED_AT=datetime.now()).execute()

                    # Insertion to photos table.
                    if photoNum != 0:
                        for n in range(photoNum):
                            photoURL = "https://ws-public.interpol.int/notices/v1/red/{}/images/{}".format(
                                entity_id.replace("/", "-"),
                                data_image["_embedded"]["images"][n]["picture_id"])
                            urllib.request.urlretrieve(photoURL, "image")
                            img = Image.open("image").tobytes()
                            # buffer = io.BytesIO()
                            # img.save(buffer, format='JPEG', quality=75)
                            # memory_of_the_image = buffer.getbuffer()

                            Photos.insert(criminal_id=int(str(Criminals.get_or_none(entity_id=entity_id))),
                                          photo_url=photoURL, photo=img,
                                          CREATED_AT=datetime.now()).execute()

                except Exception as error:
                    print("ERROR", error.__class__.__qualname__, error)

        if conv_url_x == "{}".format(TOTAL_PUBLIC_PAGES):

            # Last, if there are any records that have been deleted from Interpol, performs that deletion to tables.
            # We do not delete that record from our database, we only update that records' DELETED_AT information. All data keep staying in tables.
            if records_to_delete != 0:
                last_id = int(str(Criminals.select().order_by(Criminals.CREATED_AT.desc())[0]))
                for i in range(1, last_id + 1):
                    if Criminals.get_or_none(Criminals.id == i).DELETED_AT is None:
                        temp_entity_id = Criminals.get(Criminals.id == i).entity_id
                        if temp_entity_id not in latest_entity_ids:
                            Criminals.update({Criminals.DELETED_AT: datetime.now()}).where(
                                (Criminals.entity_id == temp_entity_id)).execute()
                            Arrest_Warrants.update({Arrest_Warrants.DELETED_AT: datetime.now()}).where(
                                Arrest_Warrants.criminal_id == Criminals.get(
                                    Criminals.entity_id == temp_entity_id)).execute()
                            Languages.update({Languages.DELETED_AT: datetime.now()}).where(
                                Languages.criminal_id == Criminals.get(Criminals.entity_id == temp_entity_id)).execute()
                            Nationalities.update({Nationalities.DELETED_AT: datetime.now()}).where(
                                Nationalities.criminal_id == Criminals.get(
                                    Criminals.entity_id == temp_entity_id)).execute()
                            Photos.update({Photos.DELETED_AT: datetime.now()}).where(
                                Photos.criminal_id == Criminals.get(Criminals.entity_id == temp_entity_id)).execute()

            print("\n\nTables have been updated successfully.")
            hasElement = False
