import json
import consts as const
# pip install BTrees
from BTrees.OOBTree import OOBTree
import pickle
import os


class Operation:
    def __init__(self):
        self.mem_table = {}
        self.ssTable = {}
        self.max_entries = 100
        self.entries = 0
        self.WALnum = 0
       
        

    def insert(self, table_name, post_data, for_WAL):
        if "table_names" not in const.manifest or table_name not in const.manifest["table_names"]["tables"]:
            return 404
        cell = {}
        try:
            cell = json.loads(post_data)
        except:
            return 400
        if cell == {}:
            return 400
        if not self.find_column_family_and_column(table_name, cell["column_family"], cell["column"]):
            return 400

        #Write to WAL
        if not for_WAL:
            dic = {}
            dic["cell"] = cell
            dic["table_name"] = table_name
            const.WAL.append(dic)
            with open(const.WAL_filename, 'wb') as outfile:
                pickle.dump(const.WAL,outfile)


        if table_name not in self.mem_table:
            self.mem_table[table_name] = {}
        if cell["column_family"] not in self.mem_table[table_name]:
            self.mem_table[table_name][cell["column_family"]] = {}
        if cell["column"] not in self.mem_table[table_name][cell["column_family"]]:
            self.mem_table[table_name][cell["column_family"]][cell["column"]] = OOBTree()
        t = self.mem_table[table_name][cell["column_family"]][cell["column"]]
        if cell["row"] not in t:
            t.update({cell["row"]: {"row": cell["row"], "data": cell["data"]}})
        else:
            t[cell["row"]]["data"] += cell["data"]
            # Garbage collection
            while len(t[cell["row"]]["data"]) > 5:
                t[cell["row"]]["data"].pop(0)
        self.entries = self.entries + 1
        if self.entries == self.max_entries:
            self.spill_to_disk()
        return 200

    def retrieve(self, table_name, get_data):
        if table_name not in const.manifest["table_names"]["tables"]:
            return {"success": False, "success_code": 404}
        input = {}
        try:
            input = json.loads(get_data)
        except:
            return {"success": False, "success_code": 400}
        if input == {}:
            return {"success": False, "success_code": 400}

        if not self.find_column_family_and_column(table_name, input["column_family"], input["column"]):
            return {"success": False, "success_code": 400}

        if table_name in self.mem_table:
            if input["column_family"] in self.mem_table[table_name]:
                if input["column"] in self.mem_table[table_name][input["column_family"]]:
                    if input["row"] in self.mem_table[table_name][input["column_family"]][input["column"]]:
                        return {"success": True,
                                "data": self.mem_table[table_name][input["column_family"]][input["column"]][
                                    input["row"]]}

        # Not found in memory
        try:
            rows = const.manifest["ssindex"][table_name][input["column_family"]][input["column"]]
        except:
            return {"success": False, "success_code": 409}
        if input["row"] not in rows:
            return {"success": False, "success_code": 409}
        for file_dict in rows[input["row"]]:
            with open(file_dict["file_name"], 'r') as file:
                file.seek(file_dict["offset"])
                return {"success": True, "data": json.loads(file.readline())}

        # for file_name in const.manifest.get("files", []):
        #     with open(file_name, 'rb') as file:
        #         sstable = pickle.load(file)
        #         if table_name in sstable:
        #             if input["column_family"] in sstable[table_name]:
        #                 if input["column"] in sstable[table_name][input["column_family"]]:
        #                     if input["row"] in sstable[table_name][input["column_family"]][input["column"]]:
        #                         return {"success": True,
        #                                 "data": sstable[table_name][input["column_family"]][input["column"]][
        #                                     input["row"]]}

        # if input["row"] not in self.ssTable:
        #     return {"success": False, "success_code": 404}
        # files = self.ssTable[input["row"]]
        # for file in files:
        #     with open(file) as json_file:
        #         data = json.load(json_file)
        #         for row in data[table_name]:
        #             if row["column_family"] == input["column_family"] and row["column"] == input["column"] and row[
        #                     "row"] == input["row"]:
        #                 return {"success": True, "data": row}
        #
        return {"success": False, "success_code": 404}

    def retrieve_cells(self, table_name, get_data):
        if table_name not in const.table_names["tables"]:
            return {"success": False, "success_code": 404}
        input = {}
        try:
            input = json.loads(get_data)
        except:
            return {"success": False, "success_code": 400}
        if input == {}:
            return {"success": False, "success_code": 400}

        if not self.find_column_family_and_column(table_name, input["column_family"], input["column"]):
            return {"success": False, "success_code": 400}

        if table_name in self.mem_table:
            if input["column_family"] in self.mem_table[table_name]:
                if input["column"] in self.mem_table[table_name][input["column_family"]]:
                    t = self.mem_table[table_name][input["column_family"]][input["column"]]
                    rows = list(t.values(input["row_from"], input["row_to"]))
                    if not rows == []:
                        return {"success": True, "data": {"rows": rows}}

        # Not found in memory
        data = []
        try:
            rows = const.manifest["ssindex"][table_name][input["column_family"]][input["column"]]
        except:
            return {"success": False, "success_code": 409}
        keys = list(rows.values(input["row_from"], input["row_to"]))
        for row in keys:
            for file_dict in row:
                with open(file_dict["file_name"], 'r') as file:
                    file.seek(file_dict["offset"])
                    data.append(json.loads(file.readline()))

        # for file_name in const.manifest.get("files",[]):
        #     with open(file_name, 'rb') as file:
        #         sstable = pickle.load(file)
        #         if table_name in sstable:
        #             if input["column_family"] in sstable[table_name]:
        #                 if input["column"] in sstable[table_name][input["column_family"]]:
        #                     t = sstable[table_name][input["column_family"]][input["column"]]
        #                     rows = list(t.values(input["row_from"], input["row_to"]))
        #                     if not rows == []:
        #                         return {"success": True, "data": {"rows": rows}}

        return {"success": True, "data": {"rows": data}}

    def set_max_entries(self, post_data):
        my_dict = {}
        try:
            my_dict = json.loads(post_data)
        except:
            return 400
        if 'memtable_max' not in my_dict:
            return 400
        new_entries = my_dict['memtable_max']

        if not isinstance(new_entries, int):
            return 400
        self.max_entries = new_entries
        if self.max_entries >= self.entries:
            self.spill_to_disk()
        return 200

    def find_column_family_and_column(self, table_name, column_family_name, column_name):
        for column_family in const.manifest["table_meta_data"][table_name]["column_families"]:
            if column_family["column_family_key"] == column_family_name:
                if column_name in column_family["columns"]:
                    return True
        return False

    def spill_to_disk(self):
        if "filenum" not in const.manifest:
            const.manifest["filenum"] = 0

        file_name = 'data' + str(const.manifest["filenum"] + 1) + '.txt'
        # pickle.dump(self.mem_table, open("save.p", "wb"))
        # entry = 0
        with open(file_name, 'w') as outfile:
            for table_name in self.mem_table:
                if table_name not in const.manifest["ssindex"]:
                    const.manifest["ssindex"].update({table_name: {}})
                for column_family in self.mem_table[table_name]:
                    if column_family not in const.manifest["ssindex"][table_name]:
                        const.manifest["ssindex"][table_name].update({column_family: {}})
                    for column in self.mem_table[table_name][column_family]:
                        if column not in const.manifest["ssindex"][table_name][column_family]:
                            const.manifest["ssindex"][table_name][column_family].update({column: OOBTree()})
                        for row in self.mem_table[table_name][column_family][column]:
                            if row not in const.manifest["ssindex"][table_name][column_family][column]:
                                const.manifest["ssindex"][table_name][column_family][column].update({row: []})
                            last_pos = outfile.tell()
                            json.dump(self.mem_table[table_name][column_family][column][row], outfile)
                            outfile.write("\n")
                            const.manifest["ssindex"][table_name][column_family][column][row].insert(0, {"file_name": file_name, "offset":last_pos})

        os.remove(const.WAL_filename)
        self.mem_table = {}
        self.entries = 0

        if "files" not in const.manifest:
            const.manifest["files"] = []


        const.manifest["files"].append(file_name)
        const.manifest["filenum"] += 1
        with open(const.manifest_filename, 'wb') as outfile:
                    pickle.dump(const.manifest, outfile)


    


