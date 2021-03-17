from bson.objectid import ObjectId


class Building:
    def __init__(self, name, abbrev, top_flr, rooms):
        self.name = name
        self.abbrev = abbrev
        self.top_flr = top_flr
        self.rooms = rooms

    def good_floor(self, n):
        if n < 0 and n <= self.top_flr:
            return True
        else:
            return False

    def good_room(self, room):
        if room in self.rooms:
            return True
        else:
            return False

    @property
    def give_name(self):
        return f"{self.abbrev}: {self.name}"


class TaggedItem:
    def __init__(self, tag_num, loc_bldg, loc_flr, loc_room,
                 make, model, serial_num, pur_date, tag_date, pur_doc,
                 dept_contact, owner, fed_prop, comment):
        self.oid = ObjectId()
        self.tag_num = tag_num
        self.loc_bldg = loc_bldg
        self.loc_flr = loc_flr
        self.loc_room = loc_room
        self.make = make
        self.model = model
        self.serial_num = serial_num
        self.pur_date = pur_date
        self.tag_date = tag_date
        self.pur_doc = pur_doc
        self.dept_contact = dept_contact
        self.owner = owner
        self.fed_prop = fed_prop
        self.comment = comment


class MapLocation:
    def __init__(self, bldg, floor, room):
        self.oid = ObjectId()
        self.bldg = bldg
        self.floor = floor
        self.room = room


class DeptContact:
    def __init__(self, contact_id, name, office_bldg, office_room, photo):
        self.oid = ObjectId()
        self.contact_id = contact_id
        self.name = name
        self.office_bldg = office_bldg
        self.office_room = office_room
        self.photo = photo


class PurchaseDocument:
    def __init__(self, document_num, document_img, associated_tags, filing_date):
        self.oid = ObjectId()
        self.document_num = document_num
        self.document_img = document_img
        self.associated_tags = associated_tags
        self.filing_date = filing_date
