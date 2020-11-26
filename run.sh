i="0"
while [ $i -lt 10 ]
do
    python3 flikr.py ./input_configurations/v1.json
    python3 flikr.py ./input_configurations/v3.json
    python3 flikr.py ./input_configurations/v2.json
done


