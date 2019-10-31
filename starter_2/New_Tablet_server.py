from http.server import HTTPServer, BaseHTTPRequestHandler
import json
from parser_util import UrlParser
from consts import Const
# import Operation as op
import pickle
import sys
from BTrees.OOBTree import OOBTree


# table_names = {"tables" : []}
# table_meta_data = {}
# mem_table = {}
# get_function_types = ['List', 'GetInfo', 'do_DELETE']


class MyHandler(BaseHTTPRequestHandler):

    # def __init__(self, request, client_address, server):
    #     self.operation = op.Operation()
    #     super().__init__(request, client_address, server)

    def _set_response(self, code):
        self.send_response(code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def do_GET(self):
        content_length = self.headers['content-length']
        get_data = {}
        if content_length != None:
            content_length = int(content_length)
            get_data = self.rfile.read(content_length)

        # data = None
        parser_get_type_obj = UrlParser('get')
        dict_return = parser_get_type_obj.parse(self.path)

        # perform on the dict_return

        # # Table Get Info
        # elif dict_return["function_name"] == const.get_function_types[1]:
        #     table_info = {}
        #     if "table_names" in const.manifest:
        #         if dict_return["table_name"] in const.manifest["table_names"]["tables"]:
        #             table_info = const.manifest["table_meta_data"][dict_return["table_name"]]
        #             data_json = json.dumps(table_info)
        #             self._set_response(200)
        #             self.wfile.write(data_json.encode("utf8"))
        #         else:
        #             self._set_response(404)
        #     else:
        #         self._set_response(400)

        # retrieve a cell
        if dict_return["function_name"] == const.get_function_types[0]:
            return_value = const.retrieve(dict_return["table_name"], get_data)
            if return_value["success"]:
                data_json = json.dumps(return_value["data"])
                self._set_response(200)
                self.wfile.write(data_json.encode("utf8"))
            else:
                self._set_response(return_value["success_code"])

        # # retrieve cells
        # elif dict_return["function_name"] == const.get_function_types[3]:
        #     return_value = const.operation.retrieve_cells(dict_return["table_name"], get_data)
        #     if return_value["success"]:
        #         data_json = json.dumps(return_value["data"])
        #         self._set_response(200)
        #         self.wfile.write(data_json.encode("utf8"))
        #     else:
        #         self._set_response(return_value["success_code"])

        # # retrieve a row
        # elif dict_return["function_name"] == const.get_function_types[4]:
        #     # It seems that we don't need to implement it
        #     pass

        # else:
        #     self._set_response(409)

    def do_POST(self):
        # example: reading content from HTTP request
        print("here")
        print(self.path)

        data = None
        content_length = self.headers['content-length']
        if content_length != None:
            content_length = int(content_length)
            post_data = self.rfile.read(content_length)

        # Create a table
        path = self.path.split('/')
        if path[1] == const.post_function_types[0]:
            print("create table")
            my_dict = {}
            try:
                my_dict = json.loads(post_data)
            except:
                self._set_response(400)
                return

            exists = True if "table_meta_data" in const.manifest and my_dict['name'] in const.manifest[
                "table_meta_data"] else False

            if exists == False:

                if "table_names" not in const.manifest:
                    const.manifest["table_names"] = const.table_names
                if "table_meta_data" not in const.manifest:
                    const.manifest["table_meta_data"] = const.table_meta_data

                # Write to manifest variable
                const.manifest["table_names"]["tables"].append(my_dict['name'])
                const.manifest["table_meta_data"][my_dict['name']] = my_dict

                # Write manifest to disk for recovery purposes
                with open(const.manifest_filename, 'wb') as outfile:
                    pickle.dump(const.manifest, outfile)

                # const.table_names["tables"].append(my_dict['name'])
                # const.table_meta_data[my_dict['name']] = my_dict
                # const.mem_table[my_dict['name']] = []

                self._set_response(200)
            else:
                self._set_response(409)
                print("409")

        # Recover
        elif path[1] == const.post_function_types[2]:
            print("recovering...")
            my_dict = {}
            try:
                my_dict = json.loads(post_data)
            except:
                self._set_response(400)
                return

            print(my_dict)

            WAL_filename = "WAL_" + my_dict["hostname"] + "_" + str(my_dict["port"]) + ".txt"
            try:
                with open(WAL_filename, 'rb') as new_file:
                    WAL = pickle.load(new_file)
                for i in range(len(WAL)):
                    if table_name not in self.manifest["table_names"]["tables"]:
                        const.manifest["table_names"]["tables"].append(table_name)
                        const.manifest["table_meta_data"][table_name] = {}

                    data_json = json.dumps(WAL[i]["cell"])
                    post_data = data_json.encode("utf8")
                    const.insert(const.WAL[i]["table_name"], post_data, True)
                with open(const.WAL_filename, 'wb') as outfile:
                    pickle.dump(const.WAL, outfile)

            except IOError:
                print("WAL file doesn't exist")
                pass

            manifest_filename = "manifest_" + my_dict["hostname"] + "_" + str(my_dict["port"]) + ".txt"
            try:
                with open(manifest_filename, 'rb') as new_file:
                    manifest = pickle.load(new_file)
                    # update ssindex
                    for table_name in manifest["ssindex"]:
                        if table_name not in const.manifest["ssindex"]:
                            const.manifest["ssindex"].update({table_name: {}})
                        for column_family in manifest["ssindex"][table_name]:
                            if column_family not in const.manifest["ssindex"][table_name]:
                                const.manifest["ssindex"][table_name].update({column_family: {}})
                            for column in manifest["ssindex"][table_name][column_family]:
                                if column not in const.manifest["ssindex"][table_name][column_family]:
                                    const.manifest["ssindex"][table_name][column_family].update({column: OOBTree()})
                                for row in manifest["ssindex"][table_name][column_family][column]:
                                    if row not in const.manifest["ssindex"][table_name][column_family][column]:
                                        const.manifest["ssindex"][table_name][column_family][column].update({row: []})
                                    const.manifest["ssindex"][table_name][column_family][column][row] += \
                                        manifest["ssindex"][table_name][column_family][column][row]
                    for table_name in manifest["table_names"]["tables"]:
                        if table_name not in const.manifest["table_names"]["tables"]:
                            const.manifest["table_names"]["tables"].append(table_name)
                    for table_name in manifest["table_meta_data"]:
                        if table_name not in const.manifest["table_meta_data"]:
                            const.manifest["table_meta_data"][table_name] = {"name": table_name, "column_families": []}
                        const.manifest["table_meta_data"][table_name]["column_families"] += \
                        manifest["table_meta_data"][table_name]["column_families"]

                with open(const.manifest_filename, 'wb') as outfile:
                    pickle.dump(const.manifest, outfile)
            except IOError:
                print("manifest file doesn't exist")
                pass
            self._set_response(200)

        # regular check
        elif path[1] == const.post_function_types[3]:
            self._set_response(200)

        else:
            parser_post_type_obj = UrlParser('post')
            dict_return = parser_post_type_obj.parse(self.path)
            # insert cell

            if dict_return["function_name"] == const.post_function_types[1]:
                print("insert cell")
                self._set_response(const.insert(dict_return["table_name"], post_data, False))

            else:
                print(dict_return["function_name"])
                self._set_response(409)

    def do_DELETE(self):
        # parser_post_type_obj = UrlParser('delete')
        # dict_return = parser_post_type_obj.parse(self.path)

        path = self.path.split('/')
        if path[1] == const.del_function_types[0]:
            if path[2] in const.manifest["table_names"]["tables"]:
                const.manifest["table_names"]["tables"].remove(path[2])
                del const.manifest["table_meta_data"][path[2]]
                with open(const.manifest_filename, 'wb') as outfile:
                    pickle.dump(const.manifest, outfile)

                self._set_response(200)
            else:
                self._set_response(404)


if __name__ == "__main__":
    if len(sys.argv) < 4:
        exit(1)

    hostname = sys.argv[1]
    port = int(sys.argv[2])

    master_hostname = sys.argv[3]
    master_port = int(sys.argv[4])

    const = Const(hostname, port)

    server_address = (hostname, port)
    master_server_address = (master_hostname, master_port)
    handler_class = MyHandler
    server_class = HTTPServer

    httpd = HTTPServer(server_address, handler_class)

    try:
        with open(const.manifest_filename, 'rb') as file:
            # const.manifest = pickle.load(file)
            pass
    except IOError:
        with open(const.manifest_filename, 'wb') as file:
            pass

    # WAL_needed = False

    try:
        with open(const.WAL_filename, 'rb') as new_file:
            # const.WAL = pickle.load(new_file)
            # WAL_needed = True
            pass
    except IOError:
        with open(const.WAL_filename, 'wb') as new_file:
            pass

    # if (WAL_needed):
    #
    #     for i in range(len(const.WAL)):
    #         data_json = json.dumps(const.WAL[i]["cell"])
    #         post_data = data_json.encode("utf8")
    #         const.insert(const.WAL[i]["table_name"], post_data, True)

    print("Tablet server running at " + hostname + " " + str(port))

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass

    httpd.server_close()
