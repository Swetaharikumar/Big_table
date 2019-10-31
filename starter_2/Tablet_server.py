from http.server import HTTPServer, BaseHTTPRequestHandler
import json
from parser_util import UrlParser
import consts as const
import Operation as op
import pickle


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
        # List Tables
        if dict_return["function_name"] == const.get_function_types[0]:
            if "table_names" in const.manifest:
                data = const.manifest["table_names"]
            else:
                data = const.table_names
            data_json = json.dumps(data)
            self._set_response(200)
            self.wfile.write(data_json.encode("utf8"))



        # Table Get Info
        elif dict_return["function_name"] == const.get_function_types[1]:
            table_info = {}
            if "table_names" in const.manifest:
                if dict_return["table_name"] in const.manifest["table_names"]["tables"]:
                    table_info = const.manifest["table_meta_data"][dict_return["table_name"]]
                    data_json = json.dumps(table_info)
                    self._set_response(200)
                    self.wfile.write(data_json.encode("utf8"))
                else:
                    self._set_response(404)
            else:
                self._set_response(400)


        # retrieve a cell
        elif dict_return["function_name"] == const.get_function_types[2]:
            return_value = const.operation.retrieve(dict_return["table_name"], get_data)
            if return_value["success"]:
                data_json = json.dumps(return_value["data"])
                self._set_response(200)
                self.wfile.write(data_json.encode("utf8"))
            else:
                self._set_response(return_value["success_code"])


        # retrieve cells
        elif dict_return["function_name"] == const.get_function_types[3]:
            return_value = const.operation.retrieve_cells(dict_return["table_name"], get_data)
            if return_value["success"]:
                data_json = json.dumps(return_value["data"])
                self._set_response(200)
                self.wfile.write(data_json.encode("utf8"))
            else:
                self._set_response(return_value["success_code"])

        # retrieve a row
        elif dict_return["function_name"] == const.get_function_types[4]:
            # It seems that we don't need to implement it
            pass

        else:
            self._set_response(409)

    def do_POST(self):
        # example: reading content from HTTP request
        data = None
        content_length = self.headers['content-length']
        if content_length != None:
            content_length = int(content_length)
            post_data = self.rfile.read(content_length)

        parser_post_type_obj = UrlParser('post')
        dict_return = parser_post_type_obj.parse(self.path)

        # Create a table
        if dict_return["function_name"] == const.post_function_types[0]:
            my_dict = {}
            try:
                my_dict = json.loads(post_data)
            except:
                self._set_response(400)
                return

            exists = True if "table_meta_data" in const.manifest and my_dict['name'] in const.manifest["table_meta_data"] else False

            if exists == False:

                if "table_names" not in const.manifest:
                    const.manifest["table_names"] = const.table_names
                if "table_meta_data" not in const.manifest:
                    const.manifest["table_meta_data"] = const.table_meta_data

                # Write to manifest variable 
                const.manifest["table_names"]["tables"].append(my_dict['name'])
                const.manifest["table_meta_data"][my_dict['name']] = my_dict


                #Write manifest to disk for recovery purposes
                with open(const.manifest_filename, 'wb') as outfile:
                    pickle.dump(const.manifest, outfile)


                # const.table_names["tables"].append(my_dict['name'])
                # const.table_meta_data[my_dict['name']] = my_dict
                #const.mem_table[my_dict['name']] = []

                self._set_response(200)
            else:
                self._set_response(409)

        # Insert cell
        elif dict_return["function_name"] == const.post_function_types[1]:
            self._set_response(const.operation.insert(dict_return["table_name"], post_data,False))

        # Set max entries
        elif dict_return["function_name"] == const.post_function_types[2]:
            my_dict = {}
            try:
                my_dict = json.loads(post_data)
            except:
                self._set_response(400)
                return

            if 'memtable_max' not in my_dict:
                return 400
            new_entries = my_dict['memtable_max']
            if not isinstance(new_entries, int):
                return 400
            const.manifest["max_entries"] = new_entries
            with open(const.manifest_filename, 'wb') as outfile:
                    pickle.dump(const.manifest, outfile)


            self._set_response(const.operation.set_max_entries(post_data))

        else:
            self._set_response(404)

    def do_DELETE(self):
        parser_post_type_obj = UrlParser('delete')
        dict_return = parser_post_type_obj.parse(self.path)

        if dict_return["function_name"] == const.del_function_types[0]:
            if dict_return["table_name"] in const.manifest["table_names"]["tables"]:
                const.manifest["table_names"]["tables"].remove(dict_return["table_name"])
                del const.manifest["table_meta_data"][dict_return["table_name"]]
                with open(const.manifest_filename, 'wb') as outfile:
                    pickle.dump(const.manifest, outfile)

                self._set_response(200)
            else:
                self._set_response(404)


if __name__ == "__main__":
    server_address = ("localhost", 8001)
    handler_class = MyHandler
    server_class = HTTPServer

    httpd = HTTPServer(server_address, handler_class)


    try:
        with open(const.manifest_filename, 'rb') as file:
            const.manifest = pickle.load(file)
    except IOError:
        with open(const.manifest_filename, 'wb') as file:
            pass

   

    WAL_needed = False

    try:
        with open(const.WAL_filename, 'rb') as new_file:
            const.WAL = pickle.load(new_file)
            WAL_needed = True
    except IOError:
        with open(const.WAL_filename, 'wb') as new_file:
            pass

    if(WAL_needed):

        for i in range(len(const.WAL)):
            data_json = json.dumps(const.WAL[i]["cell"])
            post_data = data_json.encode("utf8")
            const.operation.insert(const.WAL[i]["table_name"],post_data, True)






    print("sample server running...")

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass

    httpd.server_close()
