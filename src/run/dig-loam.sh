#!/bin/bash
#-cwd

print_usage () {
	printf "\n******************************************************\n"
	printf "dig-loam.sh [OPTIONS]\n"
	printf "\n"
	printf "REQUIRED:\n\n"
	printf "  --ls-jar [STRING]:\n"
	printf "      filename of loamstream jar\n\n"
	printf "  --ls-conf [STRING]:\n"
	printf "      filename of loamstream configuration file\n\n"
	printf "  --dig-loam [STRING]:\n"
	printf "      directory name of dig-loam github repository\n\n"
	printf "  --dig-loam-conf [STRING]:\n"
	printf "      filename of dig-loam (user) configuration file\n\n"
	printf "  --backend [STRING]:\n"
	printf "      a string from [uger,slurm] indicating which\n"
	printf "      backend to use\n\n"
	printf "  --module [STRING]:\n"
	printf "      a string from [qc,assoc] indicating which\n"
	printf "      dig-loam module to run\n\n"
	printf "  --images [STRING]:\n"
	printf "      a directory containing locally compiled singularity\n"
	printf "      images used to run dig-loam jobs\n\n"
    printf "  --tmp-dir [STRING]:\n"
	printf "      a directory to use for temporary files\n\n"
	printf "  --step [STRING]:\n"
	printf "      run a specific step (note: will only run step if prior required steps are complete)\n"
	printf "        if module == qc: prepare, harmonize, load, exportQc, annotate, kinship, ancestry, pca, sampleQc, filter, exportFinal, report\n"
	printf "        if module == assoc: crossCohortCommonVars, crossCohortPrep, crossCohortKinship, prepareSchema, prepareModel, assocTest\n\n"
	printf "  --log-level [STRING]:\n"
	printf "      a string from [TRACE,DEBUG,INFO,WARN,ERROR]\n"
	printf "      indicating the level of logging in loamstream\n\n"
	printf "  --log [STRING]:\n"
	printf "      filename of log for this dig-loam session\n\n"
	printf "OPTIONAL:\n\n"
	printf "  --protect-files [STRING]:\n"
	printf "      a filename containing a list of files to\n"
	printf "      protect from loamstream (any job with output\n"
	printf "      in this list will be skipped)\n\n"
	printf "  --enable-hashing [FLAG]:\n"
	printf "      a flag that enables hash checks by loamstream\n"
	printf "      (this may result in very long runtimes for\n"
	printf "      larger files)\n\n"
	printf "******************************************************\n\n"
}

protect_files=""
enable_hashing=0
step="all"

while [ "$1" != "" ]
do
	case $1 in
		--ls-jar)
			if [ "$2" ]; then
				ls_jar=$2
				shift
			else
				printf "\nERROR: --ls-jar requires a non-empty argument."
				exit 1
			fi
			;;
		--ls-conf)
			if [ "$2" ]; then
				ls_conf=$2
				shift
			else
				printf "\nERROR: --ls-conf requires a non-empty argument."
				exit 1
			fi
			;;
		--dig-loam)
			if [ "$2" ]; then
				dig_loam=$2
				shift
			else
				printf "\nERROR: --dig-loam requires a non-empty argument."
				exit 1
			fi
			;;
		--dig-loam-conf)
			if [ "$2" ]; then
				dig_loam_conf=$2
				shift
			else
				printf "\nERROR: --dig-loam-conf requires a non-empty argument."
				exit 1
			fi
			;;
		--backend)
			if [ "$2" ]; then
				backend=$2
				if [[ "$backend" != "uger" && "$backend" != "slurm" ]]
				then
					printf "\nERROR: --backend requires a non-empty argument from the following list: [uger,slurm]."
					exit 1
				fi
				shift
			else
				printf "\nERROR: --backend requires a non-empty argument from the following list: [uger,slurm]."
				exit 1
			fi
			;;
		--module)
			if [ "$2" ]; then
				module=$2
				if [[ "$module" != "qc" && "$module" != "assoc" ]]
				then
					printf "\nERROR: --module requires a non-empty argument from the following list: [qc,assoc]."
					exit 1
				fi
				shift
			else
				printf "\nERROR: --module requires a non-empty argument from the following list: [qc,assoc]."
				exit 1
			fi
			;;
		--images)
			if [ "$2" ]; then
				images=$2
				shift
			else
				printf "\nERROR: --images requires a non-empty argument."
				exit 1
			fi
			;;
		--tmp-dir)
			if [ "$2" ]; then
				tmp_dir=$2
				shift
			else
				printf "\nERROR: --tmp-dir requires a non-empty argument."
				exit 1
			fi
			;;
		--step)
			if [ "$2" ]; then
				step=$2
				if [[ "$step" != "qc" && "$module" != "assoc" ]]
				then
					printf "\nERROR: --step requires a non-empty argument from the following lists:\n"
					printf "         if module == qc: prepare, harmonize, load, exportQc, annotate, kinship, ancestry, pca, sampleQc, filter, exportFinal, report\n"
					printf "         if module == assoc: crossCohortCommonVars, crossCohortPrep, crossCohortKinship, prepareSchema, prepareModel, assocTest"
					exit 1
				fi
				shift
			else
				shift
			fi
			;;
		--log-level)
			if [ "$2" ]; then
				log_level=$2
				if [[ "$log_level" != "TRACE" && "$log_level" != "DEBUG" && "$log_level" != "INFO" && "$log_level" != "WARN" && "$log_level" != "ERROR" ]]
				then
					printf "\nERROR: --log-level requires a non-empty argument from the following list: [TRACE,DEBUG,INFO,WARN,ERROR]."
					exit 1
				fi
				shift
			else
				printf "\nERROR: --log-level requires a non-empty argument from the following list: [TRACE,DEBUG,INFO,WARN,ERROR]."
				exit 1
			fi
			;;
		--log)
			if [ "$2" ]; then
				log=$2
				shift
			else
				printf "\nERROR: --log requires a non-empty argument."
				exit 1
			fi
			;;
		--protect-files)
			if [ "$2" ]; then
				protect_files=$2
				shift
			else
				shift
			fi
			;;
		--enable-hashing)
			if [ "$2" ]; then
				enable_hashing=1
				shift
			else
				shift
			fi
			;;
		--help|-h)
			print_usage
			exit 0
			;;
		*)
			>&2 printf "\nERROR: invalid arguments ${1}\n"
			print_usage
			exit 1
			;;
	esac
	shift
done

echo "******************************************************" > $log
echo "dig-loam.sh" >> $log
echo "--ls-jar $ls_jar" >> $log
echo "--ls-conf $ls_conf" >> $log
echo "--dig-loam $dig_loam" >> $log
echo "--dig-loam-conf $dig_loam_conf" >> $log
echo "--backend $backend" >> $log
echo "--module $module" >> $log
echo "--images $images" >> $log
echo "--tmp-dir $tmp_dir" >> $log
echo "--step $step" >> $log
echo "--log-level $log_level" >> $log
echo "--log $log" >> $log

if [ "$module" == "qc" ]
then
	steps=$("all" "prepare" "harmonize" "load" "exportQc" "annotate" "kinship" "ancestry" "pca" "sampleQc" "filter" "exportFinal" "report")
else
	steps=$("all" "crossCohortCommonVars" "crossCohortPrep" "crossCohortKinship" "prepareSchema" "prepareModel" "assocTest")
fi

if [[ ! $(printf '%s\0' "${steps[@]}" | grep -Fxqz -- $step) ]]
then
	if [ "$module" == "qc" ]
	then
		echo "\nERROR: --step must be one of prepare, harmonize, load, exportQc, annotate, kinship, ancestry, pca, sampleQc, filter, exportFinal, or report\n"
	else
		echo "\nERROR: --step must be one of crossCohortCommonVars, crossCohortPrep, crossCohortKinship, prepareSchema, prepareModel, or assocTest\n"
	fi
fi

if [ "$protect_files" == "" ]
then
	protect_files_string=""
else
	protect_files_string="--protect-files-from ${protect_files} --run ifAnyMissingOutputs"
	echo "--protect-files $protect_files" >> $log
fi

if [ $enable_hashing -eq 1 ]
then
	disable_hashing_string="--disable-hashing"
	echo "--enable-hashing" >> $log
else
	disable_hashing_string=""
fi

echo "******************************************************" >> $log
echo $ls_jar | awk '{print "loamstream jar: "$0}' >> $log
ls_version=`java -Xmx6G -Xss4m -jar $ls_jar --version | grep "built on" | cut -d' ' -f6- | tr -dc '[:alnum:][:space:]().:\-\n' | sed 's/Z0m//g'`
echo "loamstream version: ${ls_version}" >> $log
echo "******************************************************" >> $log
echo $dig_loam | awk '{print "dig-loam path: "$0}' >> $log
dig_loam_branch=`git --git-dir ${dig_loam}/.git rev-parse --abbrev-ref HEAD`
dig_loam_commit=`git --git-dir ${dig_loam}/.git log | head -1 | cut -d' ' -f2-`
dig_loam_author=`git --git-dir ${dig_loam}/.git log | head -2 | tail -1 | cut -d' ' -f2-`
dig_loam_date=`git --git-dir ${dig_loam}/.git log | head -3 | tail -1 | cut -d' ' -f2-`
dig_loam_version="dig-loam branch: ${dig_loam_branch} commit: ${dig_loam_commit} author: ${dig_loam_author} date: ${dig_loam_date}"
echo "dig-loam version: ${dig_loam_version}" >> $log
echo "******************************************************" >> $log
echo "" >> $log

java -Xmx6G -Xss1G \
-Dloamstream-log-level=${log_level} \
-DdataConfig=${dig_loam_conf} \
-DimagesDir=${images} \
-DscriptsDir=${dig_loam}/src/scripts \
-DloamstreamVersion="${ls_version}" \
-DpipelineVersion="${dig_loam_version}" \
-DtmpDir=${tmp_dir} \
-Dstep=${step}
-jar $ls_jar \
--backend ${backend} \
--conf ${ls_conf} \
$protect_files_string \
$disable_hashing_string \
--disable-hashing \
--loams ${dig_loam}/src/scala/${module}/*.scala \
2>&1 | tee -a $log
