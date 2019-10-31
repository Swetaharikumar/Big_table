import json
import consts as const
# pip install BTrees
from BTrees.OOBTree import OOBTree
import pickle
import os
# import Operation as op


class Const:
    def __init__(self, hostname, port):
        self.table_names = {"tables": []}
        self.table_meta_data = {}
        self.manifest = {"ssindex": {}, "table_names": {"tables": []}, "table_meta_data": {}, "filenum": 0, "files": []}
        # post_function_types = ['Create Table', 'Insert Cell', "Set Max Entries"]
        # get_function_types = ['List Table', 'Get Table Info', 'Retrieve a Cell', 'Retrieve Cells', 'Retrieve a Row']
        # del_function_types = ['Delete Table']

        self.get_function_types = ['Retrieve a Cell', 'Retrieve Cells', 'Retrieve a Row']
        self.post_function_types = ['Create', 'Insert Cell', 'Recover', 'Check']
        self.del_function_types = ['Delete']

        self.manifest_filename = "manifest_" + hostname + "_" + str(port) + ".txt"
        self.WAL_filename = "WAL_" + hostname + "_" + str(port) + ".txt"
        self.WAL = []
        self.data_prefix = "data_" + hostname + "_" + str(port) + "_"
        # self.operation = op.Operation()

        self.mem_table = {}
        self.ssTable = {}
        self.max_entries = 100
        self.entries = 0
        self.WALnum = 0

    def insert(self, table_name, post_data, recover):
        if table_name not in self.manifest["table_names"]["tables"]:
            return 404

        cell = {}
        try:
            cell = json.loads(post_data)
        except:
            print(post_data)
            return 400
        if cell == {}:
            return 400
        if not self.find_column_family_and_column(table_name, cell["column_family"], cell["column"], recover):
            return 400

        # Write to WAL
        dic = {}
        dic["cell"] = cell
        dic["table_name"] = table_name
        self.WAL.append(dic)
        with open(self.WAL_filename, 'wb') as outfile:
            pickle.dump(self.WAL, outfile)

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
        if table_name not in self.manifest["table_names"]["tables"]:
            return {"success": False, "success_code": 404}
        input = {}
        try:
            input = json.loads(get_data)
        except:
            return {"success": False, "success_code": 400}
        if input == {}:
            return {"success": False, "success_code": 400}

        if not self.find_column_family_and_column(table_name, input["column_family"], input["column"], False):
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
            rows = self.manifest["ssindex"][table_name][input["column_family"]][input["column"]]
        except:
            return {"success": False, "success_code": 409}
        if input["row"] not in rows:
            return {"success": False, "success_code": 409}
        for file_dict in rows[input["row"]]:
            with open(file_dict["file_name"], 'r') as file:
                file.seek(file_dict["offset"])
                return {"success": True, "data": json.loads(file.readline())}

        return {"success": False, "success_code": 404}

    def retrieve_cells(self, table_name, get_data):
        if table_name not in self.table_names["tables"]:
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
            rows = self.manifest["ssindex"][table_name][input["column_family"]][input["column"]]
        except:
            return {"success": False, "success_code": 409}
        keys = list(rows.values(input["row_from"], input["row_to"]))
        for row in keys:
            for file_dict in row:
                with open(file_dict["file_name"], 'r') as file:
                    file.seek(file_dict["offset"])
                    data.append(json.loads(file.readline()))

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

    def find_column_family_and_column(self, table_name, column_family_name, column_name, recover):
        for column_family in self.manifest["table_meta_data"][table_name]["column_families"]:
            if column_family["column_family_key"] == column_family_name:
                if column_name in column_family["columns"]:
                    return True
                elif recover:
                    column_family["columns"].append(column_name)
                    return True
                else:
                    return False
        if recover:
            self.manifest["table_meta_data"][table_name]["column_families"].append({"column_family_key": column_family_name, "columns": [column_name]})
            return True
        else:
            return False

    def spill_to_disk(self):
        file_name = self.data_prefix + str(self.manifest["filenum"] + 1) + '.txt'
        # pickle.dump(self.mem_table, open("save.p", "wb"))
        # entry = 0
        with open(file_name, 'w') as outfile:
            for table_name in self.mem_table:
                if table_name not in self.manifest["ssindex"]:
                    self.manifest["ssindex"].update({table_name: {}})
                for column_family in self.mem_table[table_name]:
                    if column_family not in self.manifest["ssindex"][table_name]:
                        self.manifest["ssindex"][table_name].update({column_family: {}})
                    for column in self.mem_table[table_name][column_family]:
                        if column not in self.manifest["ssindex"][table_name][column_family]:
                            self.manifest["ssindex"][table_name][column_family].update({column: OOBTree()})
                        for row in self.mem_table[table_name][column_family][column]:
                            if row not in self.manifest["ssindex"][table_name][column_family][column]:
                                self.manifest["ssindex"][table_name][column_family][column].update({row: []})
                            last_pos = outfile.tell()
                            json.dump(self.mem_table[table_name][column_family][column][row], outfile)
                            outfile.write("\n")
                            self.manifest["ssindex"][table_name][column_family][column][row].insert(0, {
                                "file_name": file_name, "offset": last_pos})

        os.remove(self.WAL_filename)
        self.mem_table = {}
        self.entries = 0
        self.manifest["files"].append(file_name)
        self.manifest["filenum"] += 1
        with open(self.manifest_filename, 'wb') as outfile:
            pickle.dump(self.manifest, outfile)