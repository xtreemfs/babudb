# export GDFONTPATH=/usr/share/fonts/truetype/freefont/
set terminal png font FreeSans 12 nocrop enhanced size 700,500 

# Throughput when scanning the entire index
set ylabel "Scan Throughput (entries/s)"
set xlabel "Entries"
#set xlabel "Lookup Throughput (lookups/s)"
set output "experiments/diskindex/full_scan_thruput.png"
#set yrange [*:*]
#set xrange [*:5000000]
set logscale x
plot "experiments/diskindex/stats_num_entries.dat" using 1:11:12:13 lt 1 w errorlines t "\wo compression",\
"experiments/diskindex/stats_compression_num_entries.dat" using 1:11:12:13 lt 2 w errorlines t "\w compression"


# Time per scan for x random scans
set ylabel "Scans/s"
set xlabel "Entries"
set output "experiments/diskindex/small_scan_thruput.png"
#set yrange [*:*]
#set xrange [*:5000000]
set logscale x
plot "experiments/diskindex/stats_num_entries.dat" using 1:19:20:21 lt 1 w errorlines t "\wo compression",\
"experiments/diskindex/stats_compression_num_entries.dat" using 1:19:20:21 lt 2 w errorlines t "\w compression"

# Throughput when scanning the entire index
set ylabel "Scan Throughput (entries/s)"
set xlabel "Blocksize"
#set xlabel "Lookup Throughput (lookups/s)"
set output "experiments/diskindex/full_scan_thruput_blocks.png"
#set yrange [*:*]
#set xrange [*:5000000]
set logscale x 2
plot "experiments/diskindex/stats_blocksize.dat" using 23:11:12:13 lt 1 w errorlines t "\wo compression",\
"experiments/diskindex/stats_compression_blocksize.dat" using 23:11:12:13 lt 2 w errorlines t "\w compression"


# Time per scan for x random scans
set ylabel "Scans/s"
set xlabel "Blocksize"
set output "experiments/diskindex/small_scan_thruput_blocks.png"
#set yrange [*:*]
#set xrange [*:5000000]
set logscale x 2
plot "experiments/diskindex/stats_blocksize.dat" using 23:19:20:21 lt 1 w errorlines t "\wo compression",\
"experiments/diskindex/stats_compression_blocksize.dat" using 23:19:20:21 lt 2 w errorlines t "\w compression"

