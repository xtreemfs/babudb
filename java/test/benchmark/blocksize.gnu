set terminal png font FreeSans 12 nocrop enhanced size 700,500 

# Block size vs. scan time
set xlabel "Block size"
set ylabel "Full Scan (ms)"
set output "experiments/diskindex/blocksize.png"
#set yrange [*:*]
set xrange [1:6000]
set logscale x 2
#set format x "2^{%L}" 
plot "experiments/diskindex/stats_blocksize.dat" using 7:3 w l lt 1 t ""
#"experiments/plots/scalability/tmp/scalability_active.dat" using 1:3 w l t "active", \
#"experiments/plots/scalability/tmp/scalability_active_passive.dat" using 1:3 pt 8 t "a + p", \
#"experiments/plots/scalability/tmp/scalability_passive.dat" using 1:3 pt 4 t "passive"
