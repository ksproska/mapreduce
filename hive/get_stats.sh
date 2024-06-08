cat journal.log | grep -E "exit status|Time taken" | sed -e 's/.*Time taken: \(.*\) seconds/\1/' | awk '
{
    if ($0 ~ /\.sql - exit status:/) {
        print total, "-", $0;
        total=0;  # Reset total for next accumulation
    } else if ($1+0 == $1) {  # Check if the line is a number and add it to total
        total += $1;
    } else {
        print $0;  # Print other lines as is, e.g., messages without time information
    }
}'
