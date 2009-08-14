set terminal png font FreeSans 12 nocrop enhanced size 700,500 

# Scatter plot of prefix iteration of full index
set xlabel "Entries"
set ylabel "Full Scan (ms)"
set output "experiments/diskindex/scatter.png"
#set yrange [*:*]
set xrange [*:5000000]
set logscale x
plot "experiments/diskindex/stats_blocksize_num_entries_keylength.dat" using 1:3 pt 1 t "scatter"
