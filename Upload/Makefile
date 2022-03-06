.PHONY: check cleanse merge transform extract clean

check:
	pylint transform_cleanse.py merge_denormalize.py
	black --diff transform_cleanse.py merge_denormalize.py

cleanse:
	for YEAR in 2017 2018 2019 2020 ; \
	do \
		./transform_cleanse.py < hourly_88101_$$YEAR.csv | \
			./sort_csv.sh > pm_$$YEAR.csv ; \
		./transform_cleanse.py < hourly_RH_DP_$$YEAR.csv | \
			./sort_csv.sh > rh_$$YEAR.csv ; \
	done

merge:
	for YEAR in 2017 2018 2019 2020 ; \
	do \
		./merge_denormalize.py --pm pm_$$YEAR.csv \
			--rh rh_$$YEAR.csv --out $$YEAR.json ; \
	done

transform: cleanse merge

extract:
	curl -O https://aqs.epa.gov/aqsweb/airdata/hourly_88101_2017.zip
	curl -O https://aqs.epa.gov/aqsweb/airdata/hourly_88101_2018.zip
	curl -O https://aqs.epa.gov/aqsweb/airdata/hourly_88101_2019.zip
	curl -O https://aqs.epa.gov/aqsweb/airdata/hourly_88101_2020.zip
	curl -O https://aqs.epa.gov/aqsweb/airdata/hourly_RH_DP_2017.zip
	curl -O https://aqs.epa.gov/aqsweb/airdata/hourly_RH_DP_2018.zip
	curl -O https://aqs.epa.gov/aqsweb/airdata/hourly_RH_DP_2019.zip
	curl -O https://aqs.epa.gov/aqsweb/airdata/hourly_RH_DP_2020.zip
	unzip *.zip

all:	check transform

clean:
	rm -f *.json pm_*.csv rh_*.csv
