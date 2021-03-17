import pymongo
import classes
from bson.objectid import ObjectId


class TaggingDAO:
    def __init__(self):
        pass

    # Setting up the MongoDB connections and assign the db and collection variables
    client = pymongo.MongoClient('localhost', 27017)
    db = client.get_database("tagging")
    col = db.get_collection("taggedItems")


    @staticmethod
    def list_all_items():
        for x in TaggingDAO.col.find():
            print("************************************************")
            print("Tag Number....." + x["tag_num"])
            print("Item..........." + x["make"] + " " + x["model"])
            print("Serial Number.." + x["serial_number"])
            print("Location......." + x["location_bldg"] + " " + x["location_room"])
            print("Owner.........." + x["owner"])
            print("Inv. Rep......." + x["dept_contact"])
            print("Purchased......" + x["purchase_date"])
            print("  on Document.." + x["purchase_doc"])
            print("Tagged........." + x["tag_date"])
            print("Comment: " + x["comment"])
            print("************************************************")

    @staticmethod
    def add_one(add: classes.TaggedItem):
        print(TaggingDAO.col.insert_one({
            "_id": ObjectId(),
            "tag_num": add.tag_num,
            "location_bldg": add.loc_bldg,
            "location_flr": add.loc_flr,
            "location_room": add.loc_room,
            "make": add.make,
            "model": add.model,
            "serial_number": add.serial_num,
            "purchase_date": add.pur_date,
            "tag_date": add.tag_date,
            "purchase_doc": add.pur_doc,
            "dept_contact": add.dept_contact,
            "owner": add.owner,
            "federal_property": add.fed_prop,
            "comment": add.comment
        }))


    @staticmethod
    def exists(search_item: classes.TaggedItem):
        query = {"tag_num": search_item.tag_num}
        print(TaggingDAO.col.find(query))


    @staticmethod
    def exists(search_num: str):
        query = {"tag_num": search_num}
        print(TaggingDAO.col.find(query))


    @staticmethod
    def remove_one(rm_item: classes.TaggedItem):
        print(TaggingDAO.col.find_one_and_delete({"tag_num": rm_item.tag_num}))


    @staticmethod
    def remove_one(tag_num: str):
        print(TaggingDAO.col.find_one_and_delete({"tag_num": tag_num}))


    @staticmethod
    def remove_all():
        print(TaggingDAO.col.delete_many({}))
