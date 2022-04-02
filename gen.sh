
for i in {0..100}
do
  echo "{\"id\":$i,\"msg\":\"Messsage_$i\"}" | kcat -b localhost:9093 -P -t test-topic -k $i
done