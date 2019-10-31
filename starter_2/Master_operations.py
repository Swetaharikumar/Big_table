import json
import Master_consts as master_const
import sys




class MasterOperations:


  #finds the least loaded tablet server
	def load_balance(self):
		min = sys.maxsize
		selected_server = ""
		for server in master_const.server_load_dict:
			if master_const.server_load_dict[server] < min:
				selected_server = server
				min = master_const.server_load_dict[server]
		return selected_server

