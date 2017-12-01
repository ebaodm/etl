date
ls | egrep '^[0-9]+\.txt$' | while read line
do
	perl ../productjson.pl ./$line
done
date
