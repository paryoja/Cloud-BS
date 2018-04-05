#!/bin/bash

cd ..
git pull
OUT=$?
if [ $OUT -eq "1" ]
then
	echo "Check git status"
	read -p "Press any key to continue..."
fi

ant

OUT=$?
if [ $OUT -eq "1" ]
then
	echo "Error"
	read -p "Press any key to continue..."
fi


cd -
if [ ! -e uploader/ExperimentUploader/uploadExperiment.py ]
then
	echo "Downloadling uploadExperiment.py"
	mkdir uploader
	cd uploader
	git clone ssh://yjpark@147.46.143.74/home/yjpark/repository/ExperimentUploader/
else
	cd uploader/ExperimentUploader
	git pull
fi

