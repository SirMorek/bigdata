run:
	python -m analysis

clean:
	rm -r __pycache__/ derby.log metastore_db/ spark-warehouse/

style:
	yapf -i *.py
