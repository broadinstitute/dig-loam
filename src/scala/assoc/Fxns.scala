object Fxns extends loamstream.LoamFile {

  import java.io.{File, BufferedWriter, FileWriter}
  import scala.util.{Try, Success, Failure}
  import scala.util.control.NonFatal
  import scala.io.Source
  import java.nio.file.{Paths, Files}
  import java.lang.reflect.Field
  import scala.sys.process._
  import ProjectConfig._
  
  final case class CfgException(s: String) extends Exception(s)
  
  trait Debug{
    def debugVars(): Any = {
      val vars = this.getClass.getDeclaredFields
      val name = this.getClass.getName.split("\\$")(1)
      for(v <- vars) {
        v.setAccessible(true)
        println("Class: " + name + " Field: " + v.getName() + " => " + v.get(this))
      }
    }
  }
  
  def requiredStr(config: loamstream.conf.DataConfig, field: String, regex: String = ".*", default: Option[String] = None): String = {
    Try(config.getStr(field)) match {
      case Success(o) =>
        o.matches(regex) match {
          case false => throw new CfgException("requiredStr: field " + field + " value " + o + " does not match regex format " + regex)
          case true  => o
        }
      case Failure(NonFatal(e)) => 
        default match {
          case Some(s) => s
          case None    => throw new CfgException("requiredStr: field '" + field + "' not found")
        }
      case Failure(_)           => throw new CfgException("requiredStr: field '" + field + "' fatal error")
    }
  }
  
  def optionalStr(config: loamstream.conf.DataConfig, field: String, regex: String = ".*"): Option[String] = {
    Try(config.getStr(field)) match {
      case Success(o)           =>
        o.matches(regex) match {
          case false => throw new CfgException("optionalStr: field " + field + " value " + o + " does not match regex format " + regex)
          case true  => Some(o)
        }
      case Failure(NonFatal(e)) => None
      case Failure(_)           => throw new CfgException("optionalStr: field '" + field + "' fatal error")
    }
  }
  
  def getStrOrBlank(config: loamstream.conf.DataConfig, field: String, regex: String = ".*"): String = {
    Try(config.getStr(field)) match {
      case Success(o)           =>
        o.matches(regex) match {
          case false => throw new CfgException("getStrOrBlank: field " + field + " value " + o + " does not match regex format " + regex)
          case true  => o
        }
      case Failure(NonFatal(e)) => ""
      case Failure(_)           => throw new CfgException("getStrOrBlank: field '" + field + "' fatal error")
    }
  }
  
  def requiredInt(config: loamstream.conf.DataConfig, field: String, default: Option[Int] = None, min: Option[Int] = None, max: Option[Int] = None): Int = {
    Try(config.getInt(field)) match {
      case Success(o) =>
        (min, max) match {
          case (Some(m), Some(n)) =>
            o >= m && o <= n match {
              case true => o
              case false => throw new CfgException("requiredInt: field '" + field + "' must be in range [" + m.toString + ", " + n.toString + "]")
            }
          case (Some(m), None) =>
            o >= m match {
              case true => o
              case false => throw new CfgException("requiredInt: field '" + field + "' must be in range [" + m.toString + ",)")
            }
          case (None, Some(n)) =>
            o <= n match {
              case true => o
              case false => throw new CfgException("requiredInt: field '" + field + "' must be in range (," + n.toString + "]")
            }
          case _ => o
        }
      case Failure(NonFatal(e)) =>
        default match {
          case Some(i) => i
          case None    => throw new CfgException("requiredInt: field '" + field + "' not found")
        }
      case Failure(_)           => throw new CfgException("requiredInt: field '" + field + "' fatal error")
    }
  }
  
  def optionalInt(config: loamstream.conf.DataConfig, field: String, min: Option[Int] = None, max: Option[Int] = None): Option[Int] = {
    Try(config.getInt(field)) match {
      case Success(o) =>
        (min, max) match {
          case (Some(m), Some(n)) =>
            o >= m && o <= n match {
              case true => Some(o)
              case false => throw new CfgException("optionalInt: field '" + field + "' must be in range [" + m.toString + ", " + n.toString + "]")
            }
          case (Some(m), None) =>
            o >= m match {
              case true => Some(o)
              case false => throw new CfgException("optionalInt: field '" + field + "' must be in range [" + m.toString + ",)")
            }
          case (None, Some(n)) =>
            o <= n match {
              case true => Some(o)
              case false => throw new CfgException("optionalInt: field '" + field + "' must be in range (," + n.toString + "]")
            }
          case _ => Some(o)
        }
      case Failure(NonFatal(e)) => None
      case Failure(_)           => throw new CfgException("optionalInt: field '" + field + "' fatal error")
    }
  }
  
  def requiredBool(config: loamstream.conf.DataConfig, field: String, default: Option[Boolean] = None): Boolean = {
    Try(config.getBool(field)) match {
      case Success(o) => o
      case Failure(NonFatal(e)) =>
        default match {
          case Some(i) => i
          case None    => throw new CfgException("requiredBool: field '" + field + "' not found")
        }
      case Failure(_)           => throw new CfgException("requiredBool: field '" + field + "' fatal error")
    }
  }
  
  def requiredStrList(config: loamstream.conf.DataConfig, field: String, regex: String = ".*" , default: Option[Seq[String]] = None): Seq[String] = {
    Try(config.getStrList(field)) match {
      case Success(o)           =>
        for ( x <- o ) {
          x.toString.matches(regex) match {
            case false => throw new CfgException("requiredStrList: field " + field + " value " + x.toString + " does not match regex format " + regex)
            case true  => ()
          }
        }
        o.size match {
          case x if(x >= 1) => o
          case _ => throw new CfgException("requiredStrList: field " + field + " list must have at least 1 element")
        }
      case Failure(NonFatal(e)) => 
        default match {
          case Some(s) =>
            s.toString.matches(regex) match {
              case false => throw new CfgException("requiredStrList: field " + field + " value " + s.toString + " does not match regex format " + regex)
              case true  => s
            }
          case None    => throw new CfgException("requiredStrList: field '" + field + "' not found")
        }
      case Failure(_)           => throw new CfgException("requiredStrList: field '" + field + "' fatal error")
    }
  }
  
  def optionalStrList(config: loamstream.conf.DataConfig, field: String): Option[Seq[String]] = {
    Try(config.getStrList(field)) match {
      case Success(o)           => 
        o.size match {
          case x if(x >= 1) => Some(o)
          case _ => None
        }
      case Failure(NonFatal(e)) => None
      case Failure(_)           => throw new CfgException("optionalStrList: field '" + field + "' fatal error")
    }
  }
  
  def requiredObj(config: loamstream.conf.DataConfig, field: String, default: Option[loamstream.conf.DataConfig] = None): loamstream.conf.DataConfig = {
    Try(config.getObj(field)) match {
      case Success(o)           => o
      case Failure(NonFatal(e)) => 
        default match {
          case Some(s) => s
          case None    => throw new CfgException("requiredObj: field '" + field + "' not found")
        }
      case Failure(_)           => throw new CfgException("requiredObj: field '" + field + "' fatal error")
    }
  }
  
  def optionalObj(config: loamstream.conf.DataConfig, field: String): Option[loamstream.conf.DataConfig] = {
    Try(config.getObj(field)) match {
      case Success(o)           => Some(o)
      case Failure(NonFatal(e)) => None
      case Failure(_)           => throw new CfgException("optionalObj: field '" + field + "' fatal error")
    }
  }
  
  def requiredObjList(config: loamstream.conf.DataConfig, field: String, default: Option[Seq[loamstream.conf.DataConfig]] = None): Seq[loamstream.conf.DataConfig] = {
    Try(config.getObjList(field)) match {
      case Success(o)           => o
      case Failure(NonFatal(e)) => 
        default match {
          case Some(s) => s
          case None    => throw new CfgException("requiredObjList: field '" + field + "' not found")
        }
      case Failure(_)           => throw new CfgException("requiredObjList: field '" + field + "' fatal error")
    }
  }
  
  def optionalObjList(config: loamstream.conf.DataConfig, field: String): Option[Seq[loamstream.conf.DataConfig]] = {
    Try(config.getObjList(field)) match {
      case Success(o)           => 
        o.size match {
          case x if(x >= 1) => Some(o)
          case _ => None
        }
      case Failure(NonFatal(e)) => None
      case Failure(_)           => throw new CfgException("optionalObjList: field '" + field + "' fatal error")
    }
  }
  
  def checkPath(s: String): String = {
    Files.exists(Paths.get(s)) match {
      case false => throw new CfgException("checkPath: " + s + " not found")
      case true  => s
    }
  }
  
  def initDir(s: String): String = {
    val dir = Paths.get(s)
    Files.exists(dir) match {
      case false =>
        Files.createDirectory(dir)
        s
      case true  => s
    }
  }
  
  def expandChrList(chrs: Seq[String]): Seq[String] = {
    val y = for {
      x <- chrs
    } yield {
      x.matches("([1-9]|1[0-9]|2[0-1])-([2-9]|1[0-9]|2[0-2])") match {
        case true => (x.split("-").head.toInt to x.split("-").tail.head.toInt).toList.map(_.toString)
        case false => Seq(x)
      }
    }
    y.flatten
  }
  
  def checkPlinkPath(s: String): String = {
    val bed = Files.exists(Paths.get(s + ".bed")) 
    val bim = Files.exists(Paths.get(s + ".bim")) 
    val fam = Files.exists(Paths.get(s + ".fam"))
    val bedMessage = bed match {
      case true => s + ".bed exists"
  	case false => s + ".bed does not exist"
    }
    val bimMessage = bim match {
      case true => s + ".bim exists"
  	case false => s + ".bim does not exist"
    }
    val famMessage = fam match {
      case true => s + ".fam exists"
  	case false => s + ".fam does not exist"
    }
    Seq(bed, bim, fam).contains(false) match {
      case true => 
  	  throw new CfgException("checkPlinkPath: files missing from Plink fileset " + s + "\n  " + s"${bedMessage}\n  ${bimMessage}\n  ${famMessage}")
      case false => s
    }
  }

  def checkURI(s: String): String = {
    val gsutilBinaryOpt: Option[Path] = projectContext.config.googleConfig.map(_.gsutilBinary)
    require(gsutilBinaryOpt.isDefined, "Couldn't find gsutil binary path; set loamstream.googlecloud.gsutilBinary in loamstream.conf")
    val cmd = s"${gsutilBinaryOpt.get} -m ls ${s}"
    cmd.! match {
      case 0 => s
      case 1 => throw new CfgException("checkURI: " + s + " not found")
    }
  }
  
  def ifURI(s: String): Int = {
    val gsutilBinaryOpt: Option[Path] = projectContext.config.googleConfig.map(_.gsutilBinary)
    require(gsutilBinaryOpt.isDefined, "Couldn't find gsutil binary path; set loamstream.googlecloud.gsutilBinary in loamstream.conf")
    val cmd = s"${gsutilBinaryOpt.get} -m ls ${s}"
    cmd.!
  }
  
  def fileToList(s: String): Seq[String] = {
    val h = Source.fromFile(s)
    val lines = (for (line <- h.getLines()) yield line).toList
    h.close
    lines
  }
  
  def intervalToExpression(s: String, i: String, missing_false: Boolean): String = {
    val intervals = i.replace(" ","").split("\\+")
    val expressions = {
      for {
        interval <- intervals
      } yield {
        val lowerBound = interval(0)
        val upperBound = interval.takeRight(1)
        val min = interval.drop(1).dropRight(1).split(",")(0)
        val max = interval.drop(1).dropRight(1).split(",").size match {
          case 1 => ""
          case _ => interval.drop(1).dropRight(1).split(",")(1)
        }
        (min, max) match {
          case ("", _) | (_, "") => ()
          case (v1, v2) if v2 <= v1 => throw new CfgException("intervalToExpression: user supplied interval for " + s + ", " + i + ", is invalid due to max <= min")
          case _ => ()
        }
        val lowerInequality = lowerBound match {
          case '[' => ">="
          case '(' => ">"
          case _ => throw new CfgException("intervalToExpression: user supplied interval for " + s + " contains unsupported symbol " + lowerBound + s"... [, ], (, and ) are currently supported")
        }
        val upperInequality = upperBound match {
          case "]" => "<="
          case ")" => "<"
          case _ => throw new CfgException("intervalToExpression: user supplied interval for " + s + " contains unsupported symbol " + upperBound + s"... [, ], (, and ) are currently supported")
        }
        (min, max) match {
          case ("", v) => s + " " + upperInequality + " " + max
          case (v, "") => s + " " + lowerInequality + " " + min
          case _ => "(" + s + " " + lowerInequality + " " + min + ") & (" + s + " " + upperInequality + " " + max + ")"
        }
      }
    }
    val final_expr = expressions.size match {
      case x if x > 1 => expressions.map(e => "(" + e + ")").mkString(" | ")
      case _ => expressions.mkString("")
    }
    missing_false match {
      case false => "hl.cond(" + final_expr + ", True, False, missing_false = False)"
      case true => final_expr
    }
  }
  
  def booleanToExpression(s: String, i: String): String = {
    i match {
      case "true" => s + " == True"
      case "false" => s + " == False"
      case _ => throw new CfgException("booleanToExpression: user supplied value " + i + " for " + s + " is not currently supported... true and false are currently supported")
    }
  }
  
  def categoricalToExpression(s: String, i: Option[Seq[String]] = None, e: Option[Seq[String]] = None, substrings: Boolean = false): String = {
    substrings match {
      case false =>
        val incl = i match {
          case Some(_) =>
            i.get.size match {
              case n if n > 0 =>
                s match {
                  case "chr" =>
                    Some("hl.literal({'" + expandChrList(i.get).mkString("','") + "'}).contains(" + s + ")")
                  case _ =>
                    Some("hl.literal({'" + i.get.mkString("','") + "'}).contains(" + s + ")")
                }
              case _ => None
            }
          case None => None
        }
        val excl = e match {
          case Some(_) =>
            e.get.size match {
              case n if n > 0 =>
                s match {
                  case "chr" =>
                    Some("~ hl.literal({'" + expandChrList(e.get).mkString("','") + "'}).contains(" + s + ")")
                  case _ =>
                    Some("~ hl.literal({'" + e.get.mkString("','") + "'}).contains(" + s + ")")
                }
              case _ => None
            }
          case None => None
        }
        (incl, excl) match {
          case (Some(_), Some(_)) => "(" + incl.get  + ") & (" + excl.get + ")"
          case (None, Some(_)) => excl.get
          case (Some(_), None) => incl.get
          case _ => throw new CfgException("categoricalToExpression: user supplied values resulted in an empty expression")
        }
      case true =>
        val incl = i match {
          case Some(_) =>
            i.get.size match {
              case n if n > 0 =>
                s match {
                  case "chr" =>
                    Some("hl.any(lambda l: " + s + ".contains(l), hl.literal({'" + expandChrList(i.get).mkString("','") + "'}))")
                  case _ =>
                    Some("hl.any(lambda l: " + s + ".contains(l), hl.literal({'" + i.get.mkString("','") + "'}))")
                }
              case _ => None
            }
          case None => None
        }
        val excl = e match {
          case Some(_) =>
            e.get.size match {
              case n if n > 0 =>
                s match {
                  case "chr" =>
                    Some("~ hl.any(lambda l: " + s + ".contains(l), hl.literal({'" + expandChrList(e.get).mkString("','") + "'}))")
                  case _ =>
                    Some("~ hl.any(lambda l: " + s + ".contains(l), hl.literal({'" + e.get.mkString("','") + "'}))")
                }
              case _ => None
            }
          case None => None
        }
        (incl, excl) match {
          case (Some(_), Some(_)) => "(" + incl.get  + ") & (" + excl.get + ")"
          case (None, Some(_)) => excl.get
          case (Some(_), None) => incl.get
          case _ => throw new CfgException("categoricalToExpression: user supplied values resulted in an empty expression")
        }
    }
  }
  
  def variantFiltersToCliString(cfg: ProjectConfig, filters: Seq[String], cliOption: String, id: Option[String] = None): String = {
  
    val id_string = id match {
      case Some(s) => s + " "
      case None => " "
    }
  
    val id_exc = id match {
      case Some(s) => s + ": "
      case None => ""
    }
  
    val x = {
      for {
        f <- filters
      } yield {
        f match {
          case n if cfg.numericVariantFilters.map(e => e.id) contains n =>
            cliOption + " " + id_string + cfg.numericVariantFilters.filter(e => e.id == n).head.id + " " + cfg.numericVariantFilters.filter(e => e.id == n).head.field + " \"" + cfg.numericVariantFilters.filter(e => e.id == n).head.expression + "\""
          case b if cfg.booleanVariantFilters.map(e => e.id) contains b =>
            cliOption + " " + id_string + cfg.booleanVariantFilters.filter(e => e.id == b).head.id + " " + cfg.booleanVariantFilters.filter(e => e.id == b).head.field + " \"" + cfg.booleanVariantFilters.filter(e => e.id == b).head.expression + "\""
          case c if cfg.categoricalVariantFilters.map(e => e.id) contains c =>
            cliOption + " " + id_string + cfg.categoricalVariantFilters.filter(e => e.id == c).head.id + " " + cfg.categoricalVariantFilters.filter(e => e.id == c).head.field + " \"" + cfg.categoricalVariantFilters.filter(e => e.id == c).head.expression + "\""
          case d if cfg.compoundVariantFilters.map(e => e.id) contains d =>
            val ids = for {
              ff <- cfg.compoundVariantFilters.filter(e => e.id == d).head.include
            } yield {
              ff match {
                case nn if cfg.numericVariantFilters.map(e => e.id) contains nn =>
                  cfg.numericVariantFilters.filter(e => e.id == nn).head.field
                case bb if cfg.booleanVariantFilters.map(e => e.id) contains bb =>
                  cfg.booleanVariantFilters.filter(e => e.id == bb).head.field
                case cc if cfg.categoricalVariantFilters.map(e => e.id) contains cc =>
                  cfg.categoricalVariantFilters.filter(e => e.id == cc).head.field
                case _ => throw new CfgException("filtersToCliString: " + id_exc + " filters '" + ff + "' not found")
              }
            }
            cliOption + " " + id_string + cfg.compoundVariantFilters.filter(e => e.id == d).head.id + " " + ids.mkString(",") + " \"" + cfg.compoundVariantFilters.filter(e => e.id == d).head.expression + "\""
          case _ => throw new CfgException("filtersToCliString: " + id_exc + " filters '" + f + "' not found")
        }
      }
    }
    x.mkString(" ")
  
  }
  
  def variantFiltersToPrintableList(cfg: ProjectConfig, filters: Seq[String], id: Option[String] = None): Seq[String] = {
  
    var v = Seq[String]()
  
    val id_string = id match {
      case Some(s) => v = v ++ Seq(s)
      case None => ()
    }
  
    val id_exc = id match {
      case Some(s) => s + ": "
      case None => ""
    }
  
    val x = {
      for {
        f <- filters
      } yield {
        f match {
          case n if cfg.numericVariantFilters.map(e => e.id) contains n =>
            (v ++ Seq(cfg.numericVariantFilters.filter(e => e.id == n).head.id, cfg.numericVariantFilters.filter(e => e.id == n).head.field, cfg.numericVariantFilters.filter(e => e.id == n).head.expression)).mkString("\t")
          case b if cfg.booleanVariantFilters.map(e => e.id) contains b =>
            (v ++ Seq(cfg.booleanVariantFilters.filter(e => e.id == b).head.id, cfg.booleanVariantFilters.filter(e => e.id == b).head.field, cfg.booleanVariantFilters.filter(e => e.id == b).head.expression)).mkString("\t")
          case c if cfg.categoricalVariantFilters.map(e => e.id) contains c =>
            (v ++ Seq(cfg.categoricalVariantFilters.filter(e => e.id == c).head.id, cfg.categoricalVariantFilters.filter(e => e.id == c).head.field, cfg.categoricalVariantFilters.filter(e => e.id == c).head.expression)).mkString("\t")
          case d if cfg.compoundVariantFilters.map(e => e.id) contains d =>
            val ids = for {
              ff <- cfg.compoundVariantFilters.filter(e => e.id == d).head.include
            } yield {
              ff match {
                case nn if cfg.numericVariantFilters.map(e => e.id) contains nn =>
                  cfg.numericVariantFilters.filter(e => e.id == nn).head.field
                case bb if cfg.booleanVariantFilters.map(e => e.id) contains bb =>
                  cfg.booleanVariantFilters.filter(e => e.id == bb).head.field
                case cc if cfg.categoricalVariantFilters.map(e => e.id) contains cc =>
                  cfg.categoricalVariantFilters.filter(e => e.id == cc).head.field
                case _ => throw new CfgException("variantFiltersToPrintableList: " + id_exc + " filters '" + ff + "' not found")
              }
            }
            (v ++ Seq(cfg.compoundVariantFilters.filter(e => e.id == d).head.id, ids.mkString(","), cfg.compoundVariantFilters.filter(e => e.id == d).head.expression)).mkString("\t")
          case _ => throw new CfgException("variantFiltersToPrintableList: " + id_exc + " filters '" + f + "' not found")
        }
      }
    }
    x
  
  }
  
  def sampleFiltersToPrintableList(cfg: ProjectConfig, filters: Seq[String]): Seq[String] = {
  
    val x = {
      for {
        f <- filters
      } yield {
        f match {
          case n if cfg.numericSampleFilters.map(e => e.id) contains n =>
            Seq(cfg.numericSampleFilters.filter(e => e.id == n).head.id, cfg.numericSampleFilters.filter(e => e.id == n).head.field, cfg.numericSampleFilters.filter(e => e.id == n).head.expression).mkString("\t")
          case b if cfg.booleanSampleFilters.map(e => e.id) contains b =>
            Seq(cfg.booleanSampleFilters.filter(e => e.id == b).head.id, cfg.booleanSampleFilters.filter(e => e.id == b).head.field, cfg.booleanSampleFilters.filter(e => e.id == b).head.expression).mkString("\t")
          case c if cfg.categoricalSampleFilters.map(e => e.id) contains c =>
            Seq(cfg.categoricalSampleFilters.filter(e => e.id == c).head.id, cfg.categoricalSampleFilters.filter(e => e.id == c).head.field, cfg.categoricalSampleFilters.filter(e => e.id == c).head.expression).mkString("\t")
          case d if cfg.compoundSampleFilters.map(e => e.id) contains d =>
            val ids = for {
              ff <- cfg.compoundSampleFilters.filter(e => e.id == d).head.include
            } yield {
              ff match {
                case nn if cfg.numericSampleFilters.map(e => e.id) contains nn =>
                  cfg.numericSampleFilters.filter(e => e.id == nn).head.field
                case bb if cfg.booleanSampleFilters.map(e => e.id) contains bb =>
                  cfg.booleanSampleFilters.filter(e => e.id == bb).head.field
                case cc if cfg.categoricalSampleFilters.map(e => e.id) contains cc =>
                  cfg.categoricalSampleFilters.filter(e => e.id == cc).head.field
                case _ => throw new CfgException("sampleFiltersToPrintableList: filter '" + ff + "' not found")
              }
            }
            Seq(cfg.compoundSampleFilters.filter(e => e.id == d).head.id, ids.mkString(","), cfg.compoundSampleFilters.filter(e => e.id == d).head.expression).mkString("\t")
          case _ => throw new CfgException("sampleFiltersToPrintableList: filter '" + f + "' not found")
        }
      }
    }
    x
  
  }
  
  def getFilterFields(cfg: ProjectConfig, filters: Seq[String]): Seq[String] = {
  
    var x = Seq[String]()
    for {
      f <- filters
    } yield {
      f match {
        case n if cfg.numericVariantFilters.map(e => e.id) contains n =>
          x = x ++ Seq(cfg.numericVariantFilters.filter(e => e.id == n).head.field)
        case b if cfg.booleanVariantFilters.map(e => e.id) contains b =>
          x = x ++ Seq(cfg.booleanVariantFilters.filter(e => e.id == b).head.field)
        case c if cfg.categoricalVariantFilters.map(e => e.id) contains c =>
          x = x ++ Seq(cfg.categoricalVariantFilters.filter(e => e.id == c).head.field)
        case d if cfg.compoundVariantFilters.map(e => e.id) contains d =>
          for {
            ff <- cfg.compoundVariantFilters.filter(e => e.id == d).head.include
          } yield {
            ff match {
              case nn if cfg.numericVariantFilters.map(e => e.id) contains nn =>
                x = x ++ Seq(cfg.numericVariantFilters.filter(e => e.id == nn).head.field)
              case bb if cfg.booleanVariantFilters.map(e => e.id) contains bb =>
                x = x ++ Seq(cfg.booleanVariantFilters.filter(e => e.id == bb).head.field)
              case cc if cfg.categoricalVariantFilters.map(e => e.id) contains cc =>
                x = x ++ Seq(cfg.categoricalVariantFilters.filter(e => e.id == cc).head.field)
              case _ => throw new CfgException("getFilterFields: filters '" + ff + "' not found")
            }
          }
        case _ => throw new CfgException("getFilterFields: filters '" + f + "' not found")
      }
    }
    x
  }
  
  def prettyPrint(a: Any, indentSize: Int = 2, maxElementWidth: Int = 30, depth: Int = 0): String = {
    val indent      = " " * depth * indentSize
    val fieldIndent = indent + (" " * indentSize)
    val nextDepth   = prettyPrint(_: Any, indentSize, maxElementWidth, depth + 1)
    a match {
      case s: String =>
        val replaceMap = Seq(
          "\n" -> "\\n",
          "\r" -> "\\r",
          "\t" -> "\\t",
          "\"" -> "\\\""
        )
        '"' + replaceMap.foldLeft(s) { case (acc, (c, r)) => acc.replace(c, r) } + '"'
      case opt: Some[_] =>
        val resultOneLine = s"Some(${nextDepth(opt.get)})"
        if (resultOneLine.length <= maxElementWidth) return resultOneLine
        s"Some(\n$fieldIndent${nextDepth(opt.get)}\n$indent)"
      case xs: Seq[_] if xs.isEmpty =>
        xs.toString()
      case map: Map[_, _] if map.isEmpty =>
        map.toString()
      case xs: Map[_, _] =>
        val result = xs.map { case (key, value) => s"\n$fieldIndent${nextDepth(key)} -> ${nextDepth(value)}" }.toString
        "Map" + s"${result.substring(0, result.length - 1)}\n$indent)".substring(4)
      // Make Strings look similar to their literal form.
      // For an empty Seq just use its normal String representation.
      case xs: Seq[_] =>
        // If the Seq is not too long, pretty print on one line.
        val resultOneLine = xs.map(nextDepth).toString()
        if (resultOneLine.length <= maxElementWidth) return resultOneLine
        // Otherwise, build it with newlines and proper field indents.
        val result = xs.map(x => s"\n$fieldIndent${nextDepth(x)}").toString()
        result.substring(0, result.length - 1) + "\n" + indent + ")"
      // Product should cover case classes.
      case p: Product =>
        val prefix = p.productPrefix
        // We'll use reflection to get the constructor arg names and values.
        val cls    = p.getClass
        val fields = cls.getDeclaredFields.filterNot(_.isSynthetic).map(_.getName)
        val values = p.productIterator.toSeq
        // If we weren't able to match up fields/values, fall back to toString.
        if (fields.length != values.length) return p.toString
        fields.zip(values).toList match {
          // If there are no fields, just use the normal String representation.
          case Nil => p.toString
          // If there is more than one field, build up the field names and values.
          case kvps =>
            val prettyFields = kvps.map { case (k, v) => s"$k = ${nextDepth(v)}" }
            // If the result is not too long, pretty print on one line.
            val resultOneLine = s"$prefix(${prettyFields.mkString(", ")})"
            if (resultOneLine.length <= maxElementWidth) return resultOneLine
            // Otherwise, build it with newlines and proper field indents.
            s"$prefix(\n${kvps.map { case (k, v) => s"$fieldIndent$k = ${nextDepth(v)}" }.mkString(",\n")}\n$indent)"
        }
      // If we haven't specialized this type, just use its toString.
      case _ => a.toString
    }
  }
  
  def writeObject(obj: Any, filename: String): Unit = {
    val bw = new BufferedWriter(new FileWriter(new File(filename)))
    bw.write(prettyPrint(obj))
    bw.write("\n")
    bw.close()
  }
  
  //def findCycles(graph: LoamGraph): Set[Seq[Tool]] = {
  //  def findCycleStartingFrom(t: Tool): Option[Seq[Tool]] = {
  //    def walkFrom(current: Tool, soFar: Seq[Tool], seen: Set[Tool]): Option[Seq[Tool]] = {
  //      val preceding = graph.toolsPreceding(current)
  //      if(preceding.isEmpty) { None }
  //      else if(preceding.exists(seen.contains(_))) { 
  //        preceding.collectFirst { case p if seen.contains(p) => p }.map(offending => (offending +: soFar)) 
  //      }
  //      else { 
  //        val cycles = preceding.iterator.map(p => walkFrom(p, p +: soFar, seen + p)).filter(_.isDefined)
  //        cycles.toStream.headOption.flatten
  //      }
  //    }
  //    walkFrom(t, Seq(t), Set(t))
  //  }
  //  graph.tools.map(findCycleStartingFrom(_).toSet).flatten
  //}
  //
  //def pathToString(graph: LoamGraph)(path: Seq[Tool]): String = {
  //  path.map(t => s"'${graph.nameOf(t).get}'").mkString(" => ")
  //}
  
  def getHeader(f: String): Option[String] = {
    val src = io.Source.fromFile(f)
    try {
      src.getLines.find(_ => true)
    } finally {
      src.close()
    }
  }
  
  def verifyPheno(phenoFile: String, models: Seq[ConfigModel]): Unit = {
    val p = checkPath(phenoFile)
    val h = getHeader(p)
    var fp = Seq[String]()
    for {
      x <- models.map(e => e.pheno)
    } yield {
      fp = fp ++ Seq(x)
    }
    var fc = Seq[String]()
    for {
      x <- models.map(e => e.covars)
    } yield {
      x match {
        case Some(s) =>
          for {
            y <- x.get.split("\\+")
          } yield {
            fc = fc ++ Seq(y.replace("[","").replace("]",""))
          }
		case _ => ()
      }
    }
    fp = fp.distinct
    fc = fc.distinct
    h match {
      case Some(_) =>
        val mp = fp.filter(e => ! h.get.split("\t").intersect(fp).contains(e))
        val mc = fc.filter(e => ! h.get.split("\t").intersect(fc).contains(e))
        (mp.size, mc.size) match {
          case (a, 0) if a > 0 => throw new CfgException("verifyPheno: pheno/s " + mp.mkString(",") + " not found in header of pheno file " + phenoFile)
          case (a, b) if (a > 0 && b > 0) => throw new CfgException("verifyPheno: pheno/s " + mp.mkString(",") + " and covar/s " + mc.mkString(",") + " not found in header of pheno file " + phenoFile)
          case (0, b) if b > 0 => throw new CfgException("verifyPheno: covar/s " + mc.mkString(",") + " not found in header of pheno file " + phenoFile)
          case _ => println("Phenotype file " + phenoFile + " verified with pheno field/s " + fp.mkString(",") + " and covar field/s " + fc.mkString(","))
        }
      case None => throw new CfgException("verifyPheno: pheno file " + phenoFile + " is empty")
    }
  }

}
