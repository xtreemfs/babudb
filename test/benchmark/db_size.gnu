# export GDFONTPATH=/usr/share/fonts/truetype/freefont/
set terminal png font FreeSans 12 nocrop enhanced size 700,500 

# Scatter plot of prefix iteration of full index
set ylabel "Compression Ratio"
set xlabel "Total Entries"
set output "experiments/diskindex/db_size.png"
#set yrange [*:*]
#set xrange [*:5000000]
set logscale x
plot "< join experiments/diskindex/stats_num_entries.dat experiments/diskindex/stats_compression_num_entries.dat" using 1:($15/$38) lt 1 w l t "compression ratio"

#plot "experiments/diskindex/stats_num_entries.dat" using 1:($6/(1024*1024)) lt 1 w l t "\wo compression",\
#"experiments/diskindex/stats_compression_num_entries.dat" using 1:($6/(1024*1024)) lt 2 w l t "\w compression"

set ylabel "Compression Ratio"
set xlabel "Block Size"
set output "experiments/diskindex/db_size_blocks.png"
#set yrange [*:*]
#set xrange [*:5000000]
set logscale x 2
plot "< join experiments/diskindex/stats_blocksize.dat experiments/diskindex/stats_compression_blocksize.dat" using 23:($15/$38) lt 1 w l t "compression ratio"
