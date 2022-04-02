counter=$(head -n 1 counter.txt)
counter=${counter:-1}


JSON_FMT='{"id":"%d","message":"message_%d"}\n'
message='{"id":"'"$counter"'","message":"message_'"$counter"'"}\n'
#message=${printf "$JSON_FMT" "$counter" "$counter"}

echo $message | kcat -b localhost:9093 -P -t test-topic -k $counter
counter=$((counter+1))
echo $counter > counter.txt
