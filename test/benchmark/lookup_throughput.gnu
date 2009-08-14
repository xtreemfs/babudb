# export GDFONTPATH=/usr/share/fonts/truetype/freefont/
set terminal png font FreeSans 12 nocrop enhanced size 700,500 

# Scatter plot of prefix iteration of full index
set ylabel "Scan Throughput (entries/s)"
set xlabel "Total Entries"
#set xlabel "Lookup Throughput (lookups/s)"
set output "experiments/diskindex/compression.png"
#set yrange [*:*]
#set xrange [*:5000000]
set logscale x
plot "experiments/diskindex/stats_num_entries.dat" using 1:7 lt 1 w l t "\wo compression",\
"experiments/diskindex/stats_compression_num_entries.dat" using 1:7 lt 2 w l t "\w compression"