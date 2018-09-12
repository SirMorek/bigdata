run: wipe_output
	python -m analysis

clean: wipe_output
	rm -r __pycache__/ derby.log metastore_db/ spark-warehouse/

wipe_output:
	rm -rf output/
	mkdir output

style:
	yapf -i *.py
