run: wipe_output
	python -m analysis
	(head -n 1 output/part*.csv && tail -n +2 output/part*.csv | sort) > output/sorted.tsv
	(head -n 1 output2/part*.csv && tail -n +2 output2/part*.csv | sort) > output2/sorted.tsv

clean: wipe_output
	rm -r __pycache__/ derby.log metastore_db/ spark-warehouse/

wipe_output:
	rm -rf output/
	rm -rf output2/

style:
	yapf -i *.py
