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
moduleStep="all"

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
				moduleStep=$2
				shift
			else
				printf "\nERROR: --step requires a non-empty argument from the following lists:\n"
				printf "         if module == qc: prepare, harmonize, load, exportQc, annotate, kinship, ancestry, pca, sampleQc, filter, exportFinal, report\n"
				printf "         if module == assoc: crossCohortCommonVars, crossCohortPrep, crossCohortKinship, prepareSchema, prepareModel, assocTest"
				exit 1
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

echo "******************************************************" | tee $log
echo "dig-loam.sh" | tee -a $log
echo "--ls-jar $ls_jar" | tee -a $log
echo "--ls-conf $ls_conf" | tee -a $log
echo "--dig-loam $dig_loam" | tee -a $log
echo "--dig-loam-conf $dig_loam_conf" | tee -a $log
echo "--backend $backend" | tee -a $log
echo "--module $module" | tee -a $log
echo "--images $images" | tee -a $log
echo "--tmp-dir $tmp_dir" | tee -a $log
echo "--step $moduleStep" | tee -a $log
echo "--log-level $log_level" | tee -a $log
echo "--log $log" | tee -a $log

qcSteps=("all" "prepare" "harmonize" "load" "exportQc" "annotate" "kinship" "ancestry" "pca" "sampleQc" "filter" "exportFinal" "report")
assocSteps=("all" "crossCohortCommonVars" "crossCohortPrep" "crossCohortKinship" "prepareSchema" "prepareModel" "assocTest")

if [ "$module" == "qc" ]
then
	found=0
	for s in "${qcSteps[@]}"
	do
		if [ $s == "$moduleStep" ]
		then
			printf "\nrunning module qc, step %s\n\n" $moduleStep
			found=1
		fi
	done
	if [ $found -eq 0 ]
	then
		printf "\nERROR: for module qc --step must be one of prepare, harmonize, load, exportQc, annotate, kinship, ancestry, pca, sampleQc, filter, exportFinal, or report\n"
	fi
fi

if [ "$module" == "assoc" ]
then
	found=0
	for s in "${assocSteps[@]}"
	do
		if [ $s == "$moduleStep" ]
		then
			printf "\nrunning module assoc, step %s\n\n" $moduleStep
			found=1
		fi
	done
	if [ $found -eq 0 ]
	then
		printf "\nERROR: for module assoc --step must be one of crossCohortCommonVars, crossCohortPrep, crossCohortKinship, prepareSchema, prepareModel, or assocTest\n"
	fi
fi

if [ "$protect_files" == "" ]
then
	protect_files_string=""
else
	protect_files_string="--protect-files-from ${protect_files} --run ifAnyMissingOutputs"
	echo "--protect-files $protect_files" | tee -a $log
fi

if [ $enable_hashing -eq 1 ]
then
	disable_hashing_string="--disable-hashing"
	echo "--enable-hashing" | tee -a $log
else
	disable_hashing_string=""
fi

echo "******************************************************" | tee -a $log
echo $ls_jar | awk '{print "loamstream jar: "$0}' | tee -a $log
ls_version=`java -Xmx6G -Xss4m -jar $ls_jar --version | grep "built on" | cut -d' ' -f6- | tr -dc '[:alnum:][:space:]().:\-\n' | sed 's/Z0m//g'`
echo "loamstream version: ${ls_version}" | tee -a $log
echo "******************************************************" | tee -a $log
echo $dig_loam | awk '{print "dig-loam path: "$0}' | tee -a $log
dig_loam_branch=`git --git-dir ${dig_loam}/.git rev-parse --abbrev-ref HEAD`
dig_loam_commit=`git --git-dir ${dig_loam}/.git log | head -1 | cut -d' ' -f2-`
dig_loam_author=`git --git-dir ${dig_loam}/.git log | head -2 | tail -1 | cut -d' ' -f2-`
dig_loam_date=`git --git-dir ${dig_loam}/.git log | head -3 | tail -1 | cut -d' ' -f2-`
dig_loam_version="dig-loam branch: ${dig_loam_branch} commit: ${dig_loam_commit} author: ${dig_loam_author} date: ${dig_loam_date}"
echo "dig-loam version: ${dig_loam_version}" | tee -a $log
echo "dig-loam module: ${module}" | tee -a $log
echo "dig-loam step: ${moduleStep}" | tee -a $log
echo "******************************************************" | tee -a $log
echo "" | tee -a $log

java -Xmx6G -Xss1G \
-Dloamstream-log-level=${log_level} \
-DdataConfig=${dig_loam_conf} \
-DimagesDir=${images} \
-DscriptsDir=${dig_loam}/src/scripts \
-DloamstreamVersion="${ls_version}" \
-DpipelineVersion="${dig_loam_version}" \
-DtmpDir=${tmp_dir} \
-Dstep=${step} \
-jar $ls_jar \
--backend ${backend} \
--conf ${ls_conf} \
$protect_files_string \
$disable_hashing_string \
--disable-hashing \
--loams ${dig_loam}/src/scala/${module}/*.scala \
2>&1 | tee -a $log
