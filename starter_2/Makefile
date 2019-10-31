include hosts.mk

# EDIT THIS
MASTER_CMD=python3 Master.py
TABLET_CMD=python3 New_Tablet_server.py
# END EDIT REGION

# if you require any compilation, fill in this section
compile:
	echo "no compile"

grade1:
	cd grading/ && python3 grading.py 1 $(MASTER_HOSTNAME) $(MASTER_PORT)

grade2:
	python3 -m pip install --user BTrees
	cd grading_str/ && python3 grading.py 2 $(MASTER_HOSTNAME) $(MASTER_PORT)

test2:
	cd grading_str/ && python3 grading.py t Special $(MASTER_HOSTNAME) $(MASTER_PORT)

master:
	python3 before_start.py
	$(MASTER_CMD) $(MASTER_HOSTNAME) $(MASTER_PORT)

tablet1:
	$(TABLET_CMD) $(TABLET1_HOSTNAME) $(TABLET1_PORT) $(MASTER_HOSTNAME) $(MASTER_PORT)

tablet2:
	$(TABLET_CMD) $(TABLET2_HOSTNAME) $(TABLET2_PORT) $(MASTER_HOSTNAME) $(MASTER_PORT)

tablet3:
	$(TABLET_CMD) $(TABLET3_HOSTNAME) $(TABLET3_PORT) $(MASTER_HOSTNAME) $(MASTER_PORT)

.PHONY: master tablet1 tablet2 tablet3 grade1 grade2 compile

