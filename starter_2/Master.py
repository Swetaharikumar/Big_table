from http.server import HTTPServer, BaseHTTPRequestHandler
import json
from parser_util import UrlParser
import Master_consts as master_const
import pickle
from Master_support import MasterSupport
import requests
import sys
import threading
import time


class MasterHandler(BaseHTTPRequestHandler):

    # def __init__(self, request, client_address, server):
    #     self.operation = op.Operation()
    #     super().__init__(request, client_address, server)

    def _set_response(self, code):
        self.send_response(code)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def _set_content(self, response_from_tablet):
        self.wfile.write(response_from_tablet.json().encode("utf8"))

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
        if dict_return["function_name"] == master_const.get_function_types[0]:
            data = master_const.table_names
            data_json = json.dumps(data)
            self._set_response(200)
            self.wfile.write(data_json.encode("utf8"))


        # Get table info
        elif dict_return["function_name"] == master_const.get_function_types[1]:
            if dict_return["table_name"] in master_const.table_info:
                table_info = master_const.table_info[dict_return["table_name"]]
                info_json = json.dumps(table_info)
                self._set_response(200)
                self.wfile.write(info_json.encode("utf8"))
            else:
                self._set_response(404)

    def do_POST(self):
        # example: reading content from HTTP request
        if not master_const.check_start:
            master_const.check_start = True
            try:
                x = threading.Thread(target=check_tablets)
                x.start()
            except KeyboardInterrupt:
                pass
            except:
                print("Error: unable to start thread")

        data = None
        table_dict = None
        content_length = self.headers['content-length']
        if content_length != None:
            content_length = int(content_length)
            post_data = self.rfile.read(content_length)
            try:
                table_dict = json.loads(post_data)
            except:
                self._set_response(400)
                return

        parser_post_type_obj = UrlParser('post')
        dict_return = parser_post_type_obj.parse(self.path)

        # Create a table

        # Picks the least loaded tablet server
        if dict_return["function_name"] == master_const.post_function_types[0]:

            if table_dict["name"] in master_const.table_names["tables"]:
                self._set_response(409)
                return

            tablet_name = master_const.master_operation.load_balance()
            url_tablet = MasterSupport.url(master_const.tablets_info[tablet_name]["hostname"],
                                           master_const.tablets_info[tablet_name]["port"], "/Create/")
            hostandport = {}

            hostandport["hostname"] = master_const.tablets_info[tablet_name]["hostname"]
            hostandport["port"] = master_const.tablets_info[tablet_name]["port"]
            response = requests.post(url_tablet, json=table_dict)
            if (response.status_code == 200):
                master_const.table_names["tables"].append(table_dict["name"])
                master_const.server_load_dict[tablet_name] += 1
                master_const.server_table_dict[tablet_name].append(table_dict["name"])
                master_const.table_info[table_dict["name"]] = {}
                master_const.table_info[table_dict["name"]]["name"] = table_dict["name"]
                master_const.table_info[table_dict["name"]]["tablets"] = []
                master_const.table_info[table_dict["name"]]["tablets"].append(hostandport)

            self._set_response(response.status_code)

        # insert cells
        # elif dict_return["function_name"] == master_const.post_function_types[1]:
        #
        #     if table_dict["name"] in master_const.table_names["tables"]:
        #         self._set_response(409)
        #         return
        #
        #     tablet_name = master_const.master_operation.load_balance()
        #     url_tablet = MasterSupport.url(master_const.tablets_info[tablet_name]["hostname"],
        #                                    master_const.tablets_info[tablet_name]["port"], "/Create/")
        #     hostandport = {}
        #
        #     hostandport["hostname"] = master_const.tablets_info[tablet_name]["hostname"]
        #     hostandport["port"] = master_const.tablets_info[tablet_name]["port"]
        #     response = requests.post(url_tablet, json=table_dict)
        #
        #     self._set_response(response.status_code)

        # Open a lock to table
        if dict_return["function_name"] == master_const.post_function_types[3]:
            client_data = json.loads(post_data)
            if dict_return["table_name"] not in master_const.table_names["tables"]:
                self._set_response(404)
                return
            if dict_return["table_name"] not in master_const.locks:
                master_const.locks[dict_return["table_name"]] = []

            if client_data["client_id"] in master_const.locks[dict_return["table_name"]]:
                self._set_response(400)
            else:
                master_const.locks[dict_return["table_name"]].append(client_data["client_id"])
                self._set_response(200)

        # tablet start
        elif dict_return["function_name"] == master_const.post_function_types[4]:
            client_data = json.loads(post_data)
            tablet_hostname = client_data["hostname"]
            tablet_port = client_data["port"]
            tablet_name = "tablet"+str(len(master_const.running_tablets)+1)
            master_const.running_tablets.append(tablet_name)
            master_const.table_info.update({tablet_name: {"hostname": tablet_hostname, "port": tablet_port}})


    def do_DELETE(self):
        master_const.check_start = True
        data = None
        table_dict = None
        content_length = self.headers['content-length']
        if content_length != None:
            content_length = int(content_length)
            post_data = self.rfile.read(content_length)

        parser_post_type_obj = UrlParser('delete')
        dict_return = parser_post_type_obj.parse(self.path)
        # Destroy table
        if dict_return["function_name"] == master_const.del_function_types[0]:
            if dict_return["table_name"] not in master_const.table_names['tables']:
                self._set_response(404)
                return
            if dict_return["table_name"] in master_const.locks and len(
                    master_const.locks[dict_return["table_name"]]) != 0:
                self._set_response(409)
                return

            for tablet in master_const.server_table_dict:
                if dict_return["table_name"] in master_const.server_table_dict[tablet]:

                    tablet_name = tablet
                    url_tablet = MasterSupport.url(master_const.tablets_info[tablet_name]["hostname"],
                                                   master_const.tablets_info[tablet_name]["port"],
                                                   "/Delete/" + str(dict_return["table_name"] + '/'))
                    print(url_tablet)
                    response = requests.delete(url_tablet)
                    if (response.status_code == 200):
                        master_const.table_names["tables"].remove(dict_return["table_name"])
                        master_const.server_load_dict[tablet_name] -= 1
                        master_const.server_table_dict[tablet_name].remove(dict_return["table_name"])
                        del master_const.table_info[dict_return["table_name"]]
                    self._set_response(response.status_code)
                    return

        # CLose the lock
        elif dict_return["function_name"] == master_const.del_function_types[1]:
            try:
                table_dict = json.loads(post_data)
            except:
                self._set_response(400)
                return
            client_data = json.loads(post_data)

            if dict_return["table_name"] not in master_const.table_names["tables"]:
                self._set_response(404)
                return
            if dict_return["table_name"] in master_const.locks and client_data["client_id"] not in master_const.locks[
                dict_return["table_name"]]:
                self._set_response(400)
                return

            master_const.locks[dict_return["table_name"]].remove(client_data["client_id"])
            self._set_response(200)


def check_tablets():
    count = 0
    while True:
        if master_const.check_start and len(master_const.running_tablets) > 0:
            tablet_name = master_const.running_tablets[count % len(master_const.running_tablets)]
            url_check = MasterSupport.url(master_const.tablets_info[tablet_name]["hostname"],
                                          master_const.tablets_info[tablet_name]["port"],
                                          "/Check/")
            try:
                response = requests.post(url_check, timeout=5)
            except:
                # print("Dr. Evil is invading " + tablet_name + "!")
                print(tablet_name + " is down!")
                master_const.running_tablets.remove(tablet_name)

                if len(master_const.running_tablets)> 0:
                    # Another tablet server takes control
                    another_tablet_name = master_const.running_tablets[count % len(master_const.running_tablets)]
                    # print(another_tablet_name + " stands out to save the world!")
                    print(another_tablet_name + " is taking control...")
                    url_recover = MasterSupport.url(master_const.tablets_info[another_tablet_name]["hostname"],
                                                    master_const.tablets_info[another_tablet_name]["port"],
                                                    "/Recover/")
                    requests.post(url_recover, json={"hostname": master_const.tablets_info[another_tablet_name]["hostname"],
                                                     "port": master_const.tablets_info[tablet_name]["port"]})
                    master_const.server_load_dict[another_tablet_name] += master_const.server_load_dict[tablet_name]
                    master_const.server_load_dict[tablet_name] = 0
                    for table_name in master_const.server_table_dict[tablet_name]:
                        master_const.table_info[table_name]["tablets"].remove(
                            {"hostname": master_const.tablets_info[tablet_name]["hostname"],
                             "port": master_const.tablets_info[tablet_name]["port"]})
                        if {"hostname": master_const.tablets_info[another_tablet_name]["hostname"],
                            "port": master_const.tablets_info[another_tablet_name]["port"]} not in \
                                master_const.table_info[table_name]["tablets"]:
                            master_const.table_info[table_name]["tablets"].append(
                                {"hostname": master_const.tablets_info[another_tablet_name]["hostname"],
                                 "port": master_const.tablets_info[another_tablet_name]["port"]})
                    master_const.server_table_dict[another_tablet_name] += master_const.server_table_dict[tablet_name]
                    master_const.server_table_dict[tablet_name] = []
                    # print("Hero needs good rest")
                    print("recovery done")
                    time.sleep(100)

        time.sleep(5)
        count += 1


if __name__ == "__main__":
    if len(sys.argv) < 2:
        usage()
        exit(1)

    hostname = sys.argv[1]
    port = int(sys.argv[2])

    server_address = (hostname, port)
    handler_class = MasterHandler
    server_class = HTTPServer

    httpd = HTTPServer(server_address, handler_class)

    print("Master server running at " + hostname + " " + str(port))

    try:
        # x = threading.Thread(target=httpd.serve_forever)
        # x.start()
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass

    httpd.server_close()
