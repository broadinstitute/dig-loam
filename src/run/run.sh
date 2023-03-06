#!/bin/bash

ls_jar="/humgen/diabetes2/users/ryank/software/dig-loam-stream-releases/loamstream-assembly-1.4-SNAPSHOT.2022_1004.jar"
ls_conf="loamstream_template.conf"
dig_loam="/humgen/diabetes2/users/ryank/software/dig-loam-dev/dig-loam"
dig_loam_conf="qc_template.conf"
protect_files=""
log="qc.log"
log_level="INFO"
backend="uger"
disable_hashing=?
module="qc"

print_usage () {
	printf "\n****************************************************\n"
	printf "dig-loam.sh [OPTIONS]\n"
	printf "\n"
	printf "REQUIRED:\n"
	printf "  --ls-jar [STRING]:\n"
	printf "      filename of loamstream jar\n"
	printf "  --ls-conf [STRING]:\n"
	printf "      filename of loamstream configuration file\n"
	printf "  --dig-loam [STRING]:\n"
	printf "      directory name of dig-loam github repository\n"
	printf "  --dig-loam-conf [STRING]:\n"
	printf "      filename of dig-loam (user) configuration file\n"
	printf "  --log [STRING]:\n"
	printf "      filename of log for this dig-loam session\n"
	printf "  --backend [STRING]:\n"
	printf "      a string from [uger,slurm] indicating which\n"
	printf "      backend to use\n"
	printf "  --module [STRING]:\n"
	printf "      a string from [qc,assoc] indicating which\n"
	printf "      dig-loam module to run\n"
	printf "  --log-level [STRING]:\n"
	printf "      a string from [TRACE,DEBUG,INFO,WARN,ERROR]\n"
	printf "      indicating the level of logging in loamstream\n"
	printf "OPTIONAL:\n"
	printf "  --protect-files [STRING]:\n"
	printf "      a filename containing a list of files to\n"
	printf "      protect from loamstream (any job with output\n"
	printf "      in this list will be skipped)\n"
	printf "  --enable-hashing [FLAG]:\n"
	printf "      a flag that enables hash checks by loamstream\n"
	printf "      (this may result in very long runtimes for\n"
	printf "      larger files)\n"
	printf "****************************************************\n\n"
}

while :; do
	case $1 in
		--ls-jar)
			if [ "$2" ]; then
				ls_jar=$2
				shift
			else
				echo "\nERROR: --ls-jar requires a non-empty argument."
				exit 1
			fi
			;;
		--ls-conf)
			if [ "$2" ]; then
				ls_conf=$2
				shift
			else
				echo "\nERROR: --ls-conf requires a non-empty argument."
				exit 1
			fi
			;;
		--dig-loam)
			if [ "$2" ]; then
				dig_loam=$2
				shift
			else
				echo "\nERROR: --dig-loam requires a non-empty argument."
				exit 1
			fi
			;;
		--dig-loam-conf)
			if [ "$2" ]; then
				dig_loam_conf=$2
				shift
			else
				echo "\nERROR: --dig-loam-conf requires a non-empty argument."
				exit 1
			fi
			;;
		--protect-files)
			if [ "$2" ]; then
				protect_files=$2
				shift
			else
				protect_files=""
				shift
			fi
			;;
		--log)
			if [ "$2" ]; then
				log=$2
				shift
			else
				echo "\nERROR: --log requires a non-empty argument."
				exit 1
			fi
			;;
		--log-level)
			if [ "$2" ]; then
				log_level=$2
				if [ "$log_level" != "TRACE" && "$log_level" != "DEBUG" && "$log_level" != "INFO" && "$log_level" != "WARN" && "$log_level" != "ERROR" ]
				then
					echo "\nERROR: --log-level requires a non-empty argument from the following list: [TRACE,DEBUG,INFO,WARN,ERROR]."
					exit 1
				fi
				shift
			else
				echo "\nERROR: --log-level requires a non-empty argument from the following list: [TRACE,DEBUG,INFO,WARN,ERROR]."
				exit 1
			fi
			;;
		--backend)
			if [ "$2" ]; then
				backend=$2
				if [ "$backend" != "uger" && "$backend" != "slurm" ]
				then
					echo "\nERROR: --backend requires a non-empty argument from the following list: [uger,slurm]."
					exit 1
				fi
				shift
			else
				echo "\nERROR: --backend requires a non-empty argument from the following list: [uger,slurm]."
				exit 1
			fi
			;;
		--enable-hashing)
			if [ "$2" ]; then
				enable_hashing=1
				shift
			else
				enable_hashing=0
				shift
			fi
			;;
		--module)
			if [ "$2" ]; then
				module=$2
				if [ "$module" != "qc" && "$backend" != "assoc" ]
				then
					echo "\nERROR: --module requires a non-empty argument from the following list: [qc,assoc]."
					exit 1
				fi
				shift
			else
				echo "\nERROR: --module requires a non-empty argument from the following list: [qc,assoc]."
				exit 1
			fi
			;;
		--help|-h)
			print_usage
			exit 0
			;;
		*)
			>&2 printf "\nERROR: invalid arguments\n"
			print_usage
			exit 1
			;;
	esac
	shift
done

echo "--ls-jar: $ls_jar"
echo "--ls-conf: $ls_conf"
echo "--dig-loam: $dig_loam"
echo "--dig-loam-conf: $dig_loam_conf"
echo "--protect-files: $protect_files"
echo "--log: $log"
echo "--log-level: $log_level"
echo "--backend: $backend"
echo "--enable-hashing: $enable_hashing"
echo "--module: $module"

echo $ls_jar | awk '{print "loamstream jar: "$0}' > $log
java -Xmx6G -Xss4m -jar $ls_jar --version | awk '{print "loamstream version: "$0}' >> $log
echo "" >> $log
echo $dig_loam | awk '{print "dig-loam path: "$0}' >> $log
git --git-dir ${dig_loam}/.git rev-parse --abbrev-ref HEAD | awk '{print "dig-loam branch: "$0}' >> $log
git --git-dir ${dig_loam}/.git log | head -3  | awk '{print "dig-loam version: "$0}' >> $log
echo "" >> $log

if [ "$protect_files" == "" ]
then
	protect_files_string=""
else
	protect_files_string="--protect-files-from ${protect_files} --run ifAnyMissingOutputs"
fi

if [ $enable_hashing -eq 1 ]
then
	disable_hashing_string="--disable-hashing"
else
	disable_hashing_string=""
fi

java -Xmx4G -Xss1G -Dloamstream-log-level=${log_level} \
-DdataConfig=${dig_loam_conf} \
-jar $ls_jar \
--backend ${backend} \
--conf ${ls_conf} \
$protect_files_string \
$disable_hashing_string \
--disable-hashing \
--loams ${dig_loam}/src/scala/${module}/*.scala \
2>&1 | tee $log
