#!/bin/bash

# run qc pipeline
# -Dloamstream-log-level: log level, one of TRACE > DEBUG > INFO > WARN > ERROR
# -DdataConfig: user configuration file for dig-loam pipeline
java -Xmx4G -Xss1G -Dloamstream-log-level="TRACE" \
-DdataConfig="qc_template.conf" \
-jar /path/to/loamstream-assembly-1.4-SNAPSHOT.2022_1004.jar \
--backend uger \
--conf loamstream.conf \
--disable-hashing \
--protect-files-from protected_files.txt \
--run ifAnyMissingOutputs \
--loams \
/humgen/diabetes2/users/ryank/software/dig-loam-dev/dig-loam/src/scala/qc/*.scala \
2>&1 | tee qc.log

# LoamStream is a genomic analysis stack featuring a high-level language, compiler and runtime engine.
# Usage: scala loamstream.jar [options] [loam file(s)]
#        or if you have an executable produced by SBT Native Packager:
#        loamstream [options] [loam file(s)]
# Options:
# 
#   -b, --backend  <arg>...           The backend to run jobs on. Options are:
#                                     uger,lsf,slurm
#       --clean                       Deletes .loamstream/ ; Effectively the same
#                                     as using all of --clean-db, --clean-logs,
#                                     and --clean-scripts
#   -c, --clean-db                    Clean db
#       --clean-logs                  Clean logs
#       --clean-scripts               Clean generated DRM (Uger, LSF) scripts
#       --compile-only                Only compile the supplied .loam files, don't
#                                     run them
#       --conf  <arg>                 Path to config file
#       --disable-hashing             Don't hash files when determining whether a
#                                     job may be skipped.
#   -d, --dry-run                     Show what commands would be run without
#                                     running them
#   -l, --loams  <arg>...             Path(s) to loam script(s)
#   -n, --no-validation               Don't validate the graph produced by
#                                     evaluating .loam files
#   -p, --protect-files-from  <arg>   Path to file containing list of outputs to
#                                     NOT overwrite
#   -r, --run  <arg>...               How to run jobs: <everything> - always run
#                                     jobs, never skip them; <allOf> <regexes> -
#                                     run jobs if their names match ALL of the
#                                     passed regexes<anyOf> <regexes> - run jobs
#                                     if their names match ANY of the passed
#                                     regexes<noneOf> <regexes> - run jobs if
#                                     their names match NONE of the passed
#                                     regexes<ifAnyMissingOutputs> - run jobs if
#                                     any of their outputs are missing
#       --work-dir  <arg>             Path to store logs, db files, and job
#                                     metadata in, analogous to the default
#                                     .loamstream.  Only respected if --worker is
#                                     also supplied.
#   -w, --worker                      Run in worker mode (run on a DRM system on
#                                     behalf of another LS instance)
#   -h, --help                        Show help and exit
#   -v, --version                     Print version information and exit