# kudu-master
kudu-master -use_hybrid_clock=false -fs_wal_dir=/var/lib/kudu/master -fs_data_dirs=/var/lib/kudu/master &

# kudu-tserver
# use -logtostderr flag to enable console output
kudu-tserver -use_hybrid_clock=false -fs_wal_dir=/var/lib/kudu/tserver -fs_data_dirs=/var/lib/kudu/tserver &
