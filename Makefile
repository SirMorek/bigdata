run: wipe_output
	python -m analysis
	(head -n 1 output/part*.csv && tail -n +2 output/part*.csv | sort) > output/sorted.tsv

clean: wipe_output
	rm -r __pycache__/ derby.log metastore_db/ spark-warehouse/

wipe_output:
	rm -rf output/

style:
	yapf -i *.py
