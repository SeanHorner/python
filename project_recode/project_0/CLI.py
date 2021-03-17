import TaggingDAO
from project_recode.project_0.tagUploader.classes import TaggedItem
import csv


class Cli:
    def __init__(self):
        pass

    dao = TaggingDAO.TaggingDAO()

    @staticmethod
    def print_welcome():
        print("\nWelcome to the Inventory Tagging Program (ITP)\n")

    @staticmethod
    def print_options():
        print("******************************************************************")
        print("| 1: CSV Upload     : upload multiple tagged items by CSV        |")
        print("| 2: Manual Upload  : manually fill out an item's data           |")
        print("| 3: List Items     : list all items currently in the database   |")
        print("| 4: Delete All     : delete all items currently in the database |")
        print("| 5: Remove Tag     : remove item with specified tag number      |")
        print("| 6: Exit           : close the ITP                              |")
        print("******************************************************************")

    @staticmethod
    def menu():
        path = ""
        Cli.print_welcome()

        while True:
            Cli.print_options()
            user_choice = int(input("Please enter an option:  "))
            print()

            if user_choice == 1:
                try:
                    print("What is the file path?")
                    print("Default: D:/programming_projects/pycharm_projects/project_recode/project_0/test.csv")
                    path = input(": ")
                    csv_parser(path)
                except FileNotFoundError:
                    print(f"Failed to find file: {path}")
            elif user_choice == 2:
                print("Manual data entry mode initiated.")

                print("What is the tag number?")
                add_tag_num = input(": ")

                print("What is the building code?")
                add_bldg = is_good_building(input(": "))

                print("What floor is the item on?")
                add_floor = is_good_floor(add_bldg, input(": "))

                print("What room is it in?")
                add_room = input(": ")

                print("What is the item's make?")
                add_make = input(": ")

                print("What is the item's model?")
                add_model = input(": ")

                print("What is the serial number?")
                add_ser_num = input(": ")

                print("What is the purchase date?")
                add_pur_date = input("(mm/dd/yyyy): ")

                print("What is the tagging date?")
                add_tag_date = input("(mm/dd/yyyy): ")

                print("What is the purchase document number?")
                add_doc_num = input(": ")

                print("What is the department inventory rep's ID?")
                add_rep_num = input(": ")

                print("Who is the item owner?")
                add_owner = input(": ")

                print("Is this federal property?")
                add_fed_pop = is_true(input("(T/F): "))

                print("Any location/item comments?")
                add_comment = input(": ")

                add_tag = TaggedItem(
                    add_tag_num, add_bldg, add_floor, add_room, add_make,
                    add_model, add_ser_num, add_pur_date, add_tag_date,
                    add_doc_num, add_rep_num, add_owner, add_fed_pop, add_comment
                )
                Cli.dao.add_one(add_tag)
            elif user_choice == 3:
                Cli.dao.list_all_items()
            elif user_choice == 4:
                print("Are you sure you want to delete all database items?")
                if is_true(input("(T/F): ")):
                    Cli.dao.remove_all()
            elif user_choice == 5:
                print("What is the tag number of the item you'd like to remove?")
                rm_tag = input(": ")
                Cli.dao.remove_one(rm_tag)
            elif user_choice == 6:
                print("Now exiting the program.")
                break


def is_good_building(bldg_abv):
    building_list = [
        "ADH", "AF1", "AF2", "AFP", "AHG", "ANB", "AND", "ARC", "ART", "ASE", "ATT", "BAT", "BEL", "BEN", "BGH",
        "BHD", "BIO", "BLD", "BMA", "BMC", "BME", "BMK", "BMS", "BOT", "BRB", "BRG", "BSB", "BTL", "BUR", "BWY",
        "CAL", "CBA", "CCG", "CCJ", "CDA", "CDL", "CEE", "CLK", "CMA", "CMB", "CML", "COM", "CPC", "CPE", "CRB",
        "CRD", "CRH", "CS3", "CS4", "CS5", "CS6", "CS7", "CSS", "CT1", "DCP", "DEV", "DFA", "DFF", "DPI", "DTB",
        "E10", "E11", "E12", "E13", "E15", "E23", "E24", "E25", "E26", "ECG", "ECJ", "EER", "EHZ", "EPS", "ERC",
        "ETC", "FAC", "FC1", "FC2", "FC3", "FC4", "FC5", "FC6", "FC7", "FC8", "FC9", "FCS", "FDH", "FNT", "FSB",
        "G11", "G17", "GAR", "GDC", "GEA", "GEB", "GLT", "GOL", "GRC", "GRE", "GRF", "GRP", "GRS", "GSB", "GUG",
        "GWB", "HCG", "HDB", "HLB", "HMA", "HRC", "HRH", "HSM", "HTB", "IC2", "ICB", "IMA", "IMB", "IPF", "JCD",
        "JES", "JGB", "JHH", "JON", "KIN", "LAC", "LBJ", "LCD", "LCH", "LDH", "LFH", "LLA", "LLB", "LLC", "LLD",
        "LLE", "LLF", "LS1", "LTD", "LTH", "MAG", "MAI", "MB1", "MBB", "MEZ", "MFH", "MHD", "MMS", "MNC", "MRH",
        "MSB", "MTC", "N24", "NEZ", "NHB", "NMS", "NUG", "NUR", "PA1", "PA3", "PA4", "PAC", "PAI", "PAR", "PAT",
        "PB2", "PB5", "PB6", "PCL", "PH1", "PH2", "PHD", "PHR", "PMA", "POB", "PPA", "PPE", "PPL", "PRH", "RHD",
        "RHG", "RLP", "ROW", "RRH", "RSC", "SAG", "SBS", "SEA", "SER", "SJG", "SJH", "SMC", "SOF", "SRH", "SSB",
        "SSW", "STD", "SUT", "SW7", "SWG", "SZB", "TCC", "TCP", "TES", "TMM", "TNH", "TRG", "TSB", "TSC", "TSP",
        "TTC", "UA9", "UIL", "UNB", "UPB", "UTA", "UTC", "UTX", "VRX", "WAG", "WAT", "WCH", "WCP", "WCS", "WEL",
        "WGB", "WIN", "WMB", "WWH", "Z02"
    ]
    bldg = bldg_abv.upper()

    while True:
        if bldg in building_list:
            return bldg
        else:
            print(f"{bldg} is not a known abbreviation, please verify it and try again.")
            print(building_list)
            bldg = input().upper()


def is_good_floor(bldg, n):
    bldg_floor = {
        "ADH": 9, "AF1": 1, "AF2": 1, "AFP": 1, "AHG": 4, "ANB": 3, "AND": 5, "ARC": 6, "ART": 5, "ASE": 7,
        "ATT": 8, "BAT": 5, "BEL": 11, "BEN": 5, "BGH": 1, "BHD": 6, "BIO": 7, "BLD": 7, "BMA": 5, "BMC": 8,
        "BME": 10, "BMK": 2, "BMS": 5, "BOT": 2, "BRB": 5, "BRG": 8, "BSB": 1, "BTL": 7, "BUR": 8, "BWY": 3,
        "CAL": 7, "CBA": 10, "CCG": 4, "CCJ": 4, "CDA": 1, "CDL": 6, "CEE": 2, "CLK": 1, "CMA": 9, "CMB": 10,
        "CML": 1, "COM": 1, "CPC": 1, "CPE": 7, "CRB": 2, "CRD": 4, "CRH": 3, "CS3": 2, "CS4": 2, "CS5": 2,
        "CS6": 5, "CS7": 4, "CSS": 1, "CT1": 2, "DCP": 3, "DEV": 5, "DFA": 5, "DFF": 3, "DPI": 4, "DTB": 1,
        "E10": 1, "E11": 1, "E12": 1, "E13": 1, "E15": 1, "E23": 1, "E24": 1, "E25": 1, "E26": 1, "ECG": 9,
        "ECJ": 14, "EER": 9, "EHZ": 1, "EPS": 5, "ERC": 6, "ETC": 10, "FAC": 6, "FC1": 4, "FC2": 2, "FC3": 2,
        "FC4": 1, "FC5": 2, "FC6": 2, "FC7": 1, "FC8": 2, "FC9": 1, "FCS": 3, "FDH": 2, "FNT": 9, "FSB": 1,
        "G11": 1, "G17": 1, "GAR": 5, "GDC": 7, "GEA": 7, "GEB": 5, "GLT": 2, "GOL": 5, "GRC": 1, "GRE": 7,
        "GRF": 1, "GRP": 1, "GRS": 1, "GSB": 5, "GUG": 6, "GWB": 5, "HCG": 6, "HDB": 9, "HLB": 7, "HMA": 4,
        "HRC": 10, "HRH": 5, "HSM": 4, "HTB": 5, "IC2": 4, "ICB": 1, "IMA": 1, "IMB": 1, "IPF": 1, "JCD": 28,
        "JES": 7, "JGB": 7, "JHH": 3, "JON": 7, "KIN": 6, "LAC": 6, "LBJ": 10, "LCD": 1, "LCH": 2, "LDH": 2,
        "LFH": 4, "LLA": 2, "LLB": 2, "LLC": 2, "LLD": 2, "LLE": 2, "LLF": 2, "LS1": 1, "LTD": 5, "LTH": 3,
        "MAG": 6, "MAI": 37, "MB1": 1, "MBB": 5, "MEZ": 6, "MFH": 3, "MHD": 5, "MMS": 2, "MNC": 3, "MRH": 8,
        "MSB": 1, "MTC": 1, "N24": 17, "NEZ": 10, "NHB": 9, "NMS": 7, "NUG": 5, "NUR": 6, "PA1": 1, "PA3": 1,
        "PA4": 2, "PAC": 11, "PAI": 7, "PAR": 5, "PAT": 9, "PB2": 1, "PB5": 1, "PB6": 1, "PCL": 7, "PH1": 1,
        "PH2": 1, "PHD": 6, "PHR": 6, "PMA": 19, "POB": 7, "PPA": 1, "PPE": 5, "PPL": 6, "PRH": 1, "RHD": 6,
        "RHG": 6, "RLP": 7, "ROW": 1, "RRH": 10, "RSC": 2, "SAG": 7, "SBS": 2, "SEA": 8, "SER": 5, "SJG": 8,
        "SJH": 7, "SMC": 0, "SOF": 1, "SRH": 5, "SSB": 7, "SSW": 4, "STD": 10, "SUT": 5, "SW7": 2, "SWG": 6,
        "SZB": 5, "TCC": 4, "TCP": 1, "TES": 1, "TMM": 6, "TNH": 5, "TRG": 6, "TSB": 1, "TSC": 6, "TSP": 1,
        "TTC": 3, "UA9": 4, "UIL": 4, "UNB": 5, "UPB": 1, "UTA": 10, "UTC": 6, "UTX": 5, "VRX": 1, "WAG": 6,
        "WAT": 2, "WCH": 6, "WCP": 7, "WCS": 1, "WEL": 7, "WGB": 1, "WIN": 4, "WMB": 7, "WWH": 4, "Z02": 1
    }
    top = bldg_floor[bldg]
    test = int(n)

    while test > top:
        print(f"{n} is too high, the top floor is {top}.")
        print("Please verify the floor number and try again.")
        try:
            test = int(input())
        except TypeError:
            print("Please enter an integer.")
            test = int(input(": "))

    return test


def is_true(string):
    if string.casefold() == "true".casefold() or string[0].lower() == 't':
        return True
    elif string.casefold() == "yes".casefold() or string[0].lower() == 'y':
        return True
    else:
        return False


def csv_parser(path):
    with open(path, newline='') as csvfile:
        items = csv.reader(csvfile)
        for item in items:
            if len(item) < 2:
                continue
            new_tag = TaggedItem(
                item[0].strip(), item[1].strip(), item[2].strip(), item[3].strip(), item[4].strip(),
                item[5].strip(), item[6].strip(), item[7].strip(), item[8].strip(), item[9].strip(),
                item[10].strip(), item[11].strip(), is_true(str(item[12])), item[13].strip())
            Cli.dao.add_one(new_tag)
