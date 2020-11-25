i="0"

while [ $i -lt 10000 ]
do
python3 flikr.py ./input_configurations/v3.json

i=$[$i+1]
sleep 4h
done


