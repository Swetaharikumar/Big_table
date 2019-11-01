import Master_operations as mop


table_names = {"tables" : []}
table_info = {}
server_load_dict = {"tablet1":0, "tablet2" :0, "tablet3": 0}
server_table_dict = {"tablet1":[], "tablet2" :[], "tablet3" : []}
# tablets_info = {"tablet1":{"hostname": "localhost", "port": 7510}, "tablet2" : {"hostname": "localhost", "port": 7511}, "tablet3":{"hostname": "localhost", "port": 7512}}
tablets_info = {}
post_function_types = ['Create Table', 'Insert Cell', "Set Max Entries",'Open lock', 'Tablet start', 'Sharding', 'Forward']
get_function_types = ['List Table', 'Get Table Info', 'Retrieve a Cell', 'Retrieve Cells', 'Retrieve a Row']
del_function_types = ['Delete Table', 'Close lock']
locks = {}
check_start = False
running_tablets = []

master_operation = mop.MasterOperations()
