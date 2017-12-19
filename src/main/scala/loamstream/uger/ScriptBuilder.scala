package loamstream.uger

import loamstream.model.jobs.commandline.CommandLineJob

/**
  * @author Kaan
  *         Date: Jul 1, 2016
  *
  * Used to facilitate generation of bash scripts to submit task arrays to UGER
  * For an example of such scripts, see src/test/resources/imputation/shapeItUgerSubmissionScript.sh
  */
private[uger] object ScriptBuilder {
  private val space: String = " "
  private val newLine: String = "\n"
  private val unixLineSep: String = " \\"

  //NB: We need to 'use' Java-1.8 to make some steps of the QC pipeline work.  
  private val scriptHeader: String = {
    s"""|#!/bin/bash
        |#$$ -cwd
        |
        |source /broad/software/scripts/useuse
        |reuse -q UGER
        |reuse -q Java-1.8
        |
        |export PATH=/humgen/diabetes/users/dig/miniconda2/bin:$$PATH
        |conda env create -f /humgen/diabetes/users/dig/hail/environment.yml
        |source activate hail
        |
        |i=$$SGE_TASK_ID
        |      """.stripMargin
  }
  
  private val endIf: String = s"${newLine}fi${newLine}"

  def buildFrom(taskArray: UgerTaskArray): String = {
    val ugerJobs = taskArray.ugerJobs
    
    val firstIfBlock = getFirstIfBlock(taskArray)

    val elseIfBlocks = ugerJobs.tail.map { ugerJob =>
      val index = ugerJob.ugerIndex
      
      s"${getElseIfHeader(index)}${getBody(ugerJob.ugerCommandLine(taskArray))}"
    }.mkString(newLine)

    s"${scriptHeader}${newLine}${firstIfBlock}${newLine}${elseIfBlocks}${endIf}"
  }

  private def getFirstIfBlock(taskArray: UgerTaskArray): String = {
    val ugerJob: UgerJobWrapper = taskArray.ugerJobs.head
    val indexStartValue: Int = ugerJob.ugerIndex
    
    val ifHeader = getIfHeader(indexStartValue)
    val ifBody = getBody(ugerJob.ugerCommandLine(taskArray))

    s"${ifHeader}${ifBody}"
  }

  private def getBody(commandLineString: String): String = s"${newLine}${commandLineString}"

  private def getIfHeader(index: Int): String = s"if [ $$i -eq $index ]${newLine}then"

  private def getElseIfHeader(index: Int): String = s"elif [ $$i -eq $index ]${newLine}then"
}
