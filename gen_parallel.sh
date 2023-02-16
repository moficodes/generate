#!/bin/bash
# Run the commands in the background
for i in {1..10}; do
  ./generate -count 100_000_000 -file input.txt -fileindex $i &
done

# Wait for all background processes to finish
wait

cat input_*.txt > input.txt
rm input_*.txt
# Output a message indicating that all commands have completed
echo "All commands have completed"