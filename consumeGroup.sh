#kcat -b localhost:9093 -C  -f '\nKey (%K bytes): %k\t\nValue (%S bytes): %s\n\Partition: %p\tOffset: %o\n--\n'  -t test-topic

kcat -b localhost:9093 -C  -f '\nKey (%K bytes): %k\t\nValue (%S bytes): %s\n\Partition: %p\tOffset: %o\n--\n'  -G group-1 test-topic
