object Collections extends loamstream.LoamFile {

  import ProjectConfig._
  import Fxns._
  
  final case class SchemaCohort(
      schema: ConfigSchema,
      cohorts: Seq[ConfigCohort]) {
    def canEqual(a: Any) = a.isInstanceOf[SchemaCohort]
    override def equals(that: Any): Boolean = that match {
      case that: SchemaCohort => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }
    override def hashCode: Int = {
        val prime = 31
        var result = 1
        result = prime * result + schema.id.hashCode
        result = prime * result + cohorts.map(e => e.id).hashCode
        result
    }
  }
  
  //final case class SchemaMetaCohort(
  //    schema: ConfigSchema,
  //    meta: ConfigMeta,
  //    cohorts: Seq[ConfigCohort]) {
  //  def canEqual(a: Any) = a.isInstanceOf[SchemaMetaCohort]
  //  override def equals(that: Any): Boolean = that match {
  //    case that: SchemaMetaCohort => that.canEqual(this) && this.hashCode == that.hashCode
  //    case _ => false
  //  }
  //  override def hashCode: Int = {
  //      val prime = 31
  //      var result = 1
  //      result = prime * result + schema.id.hashCode
  //      result = prime * result + meta.id.hashCode
  //      result = prime * result + cohorts.map(e => e.id).hashCode
  //      result
  //  }
  //}
  
  final case class SchemaFilterField(
      schema: ConfigSchema,
      fields: Seq[String]) {
    def canEqual(a: Any) = a.isInstanceOf[SchemaFilterField]
    override def equals(that: Any): Boolean = that match {
      case that: SchemaFilterField => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }
    override def hashCode: Int = {
        val prime = 31
        var result = 1
        result = prime * result + schema.id.hashCode
        result = prime * result + fields.hashCode
        result
    }
  }
  
  final case class ModelCollection(
      model: ConfigModel,
      schema: ConfigSchema,
      cohorts: Seq[ConfigCohort],
      knowns: Option[Seq[ConfigKnown]]) {
    def canEqual(a: Any) = a.isInstanceOf[ModelCollection]
    override def equals(that: Any): Boolean = that match {
      case that: ModelCollection => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }
    override def hashCode: Int = {
        val prime = 31
        var result = 1
        result = prime * result + model.id.hashCode
        result = prime * result + schema.id.hashCode
        result = prime * result + cohorts.map(e => e.id).hashCode
        knowns match {
          case Some(_) => result = prime * result + knowns.get.map(e => e.id).hashCode
          case None => result = prime * result + None.hashCode
        }
        result
    }
  }
  
  final case class ModelMetaCollection(
      model: ConfigModel,
      schema: ConfigSchema,
      meta: ConfigMeta,
      cohorts: Seq[ConfigCohort]) {
    def canEqual(a: Any) = a.isInstanceOf[ModelMetaCollection]
    override def equals(that: Any): Boolean = that match {
      case that: ModelMetaCollection => that.canEqual(this) && this.hashCode == that.hashCode
      case _ => false
    }
    override def hashCode: Int = {
        val prime = 31
        var result = 1
        result = prime * result + model.id.hashCode
        result = prime * result + schema.id.hashCode
        result = prime * result + meta.id.hashCode
        result = prime * result + cohorts.map(e => e.id).hashCode
        result
    }
  }

  val usedArrays: Seq[String] = {
    for {
      schema <- projectConfig.Schemas
      cohort <- projectConfig.Cohorts.filter(e => schema.cohorts.contains(e.id))
    } yield {
      cohort.array
    }
  }.distinct
  
  val schemaCohorts: Seq[SchemaCohort] = {
    for {
      schema <- projectConfig.Schemas
    } yield {
      schema.design match {
        case "full" =>
          Seq(SchemaCohort(
            schema = schema,
            cohorts = projectConfig.Cohorts.filter(e => schema.cohorts.contains(e.id))
          ))
        case "strat" =>
          for {
            cohort <- projectConfig.Cohorts.filter(e => schema.cohorts.contains(e.id))
          } yield {
           SchemaCohort(
             schema = schema,
             cohorts = Seq(cohort)
           )
          }
      }
    }
  }.flatten.distinct
  
  //val schemaMetaCohorts: Seq[SchemaMetaCohort] = {
  //  for {
  //    schema <- projectConfig.Schemas.filter(e => e.metas.isDefined && e.design == "strat")
  //    meta <- projectConfig.Metas.filter(e => schema.metas.get.contains(e.id))
  //    cohort <- projectConfig.Cohorts.filter(e => meta.cohorts.contains(e.id))
  //  } yield {
  //    SchemaMetaCohort(
  //      schema = schema,
  //      meta = meta,
  //      cohorts = Seq(cohort)
  //    )
  //  }
  //}.distinct
  
  val schemaFilterFields: Seq[SchemaFilterField] = {
    for {
      schema <- projectConfig.Schemas
    } yield {
      var filterFields = Seq[String]()
      schema.filters match {
        case Some(l) => filterFields = filterFields ++ getFilterFields(cfg = projectConfig, filters = l)
        case None => ()
      }
      (schema.design, schema.cohortFilters) match {
        case ("full", Some(l)) =>
          for {
            cf <- l if schema.cohorts.contains(cf.cohort)
          } yield {
            filterFields = filterFields ++ getFilterFields(cfg = projectConfig, filters = cf.filters)
          }
        case ("strat", Some(l)) =>
          for {
            cf <- l if schema.cohorts.head == cf.cohort
          } yield {
            filterFields = filterFields ++ getFilterFields(cfg = projectConfig, filters = cf.filters)
          }
        case _ => ()
      }
      (schema.design, schema.knockoutFilters) match {
        case ("full", Some(l)) =>
          for {
            cf <- l if schema.cohorts.contains(cf.cohort)
          } yield {
            filterFields = filterFields ++ getFilterFields(cfg = projectConfig, filters = cf.filters)
          }
        case _ => ()
      }
      schema.masks match {
        case Some(l) =>
          for {
            mf <- l
          } yield {
            filterFields = filterFields ++ getFilterFields(cfg = projectConfig, filters = mf.filters)
          }
        case None => ()
      }
      SchemaFilterField(
        schema = schema,
        fields = filterFields
      )
    }
  }
  
  val modelCollections: Seq[ModelCollection] = {
    for {
      model <- projectConfig.Models
      schema <- projectConfig.Schemas.filter(e => e.id == model.schema)
    } yield {
      val knowns = model.knowns match {
        case Some(_) => Some(projectConfig.Knowns.filter(e => model.knowns.contains(e.id)))
        case None => None
      }
      schema.design match {
        case "full" =>
          Seq(ModelCollection(
            model = model,
            schema = schema,
            cohorts = projectConfig.Cohorts.filter(e => model.cohorts.contains(e.id)),
            knowns = knowns
          ))
        case "strat" =>
          for {
            cohort <- projectConfig.Cohorts.filter(e => model.cohorts.contains(e.id))
          } yield {
            ModelCollection(
              model = model,
              schema = schema,
              cohorts = Seq(cohort),
              knowns = knowns
            )
          }
      }
    }
  }.flatten.distinct
  
  val modelMetaCollections: Seq[ModelMetaCollection] = {
    for {
      model <- projectConfig.Models.filter(e => e.metas.isDefined)
      schema <- projectConfig.Schemas.filter(e => e.id == model.schema)
      meta <- projectConfig.Metas.filter(e => model.metas.get.contains(e.id))
      cohort <- projectConfig.Cohorts.filter(e => meta.cohorts.contains(e.id))
    } yield {
      ModelMetaCollection(
        schema = schema,
        model = model,
        meta = meta,
        cohorts = Seq(cohort)
      )
    }
  }.distinct
  
  //final case class SchemaCohortKnown(
  //    schema: ConfigSchema,
  //    cohorts: Seq[ConfigCohort],
  //    known: ConfigKnown) {
  //  def canEqual(a: Any) = a.isInstanceOf[SchemaCohortKnown]
  //  override def equals(that: Any): Boolean = that match {
  //    case that: SchemaCohortKnown => that.canEqual(this) && this.hashCode == that.hashCode
  //    case _ => false
  //  }
  //  override def hashCode: Int = {
  //      val prime = 31
  //      var result = 1
  //      result = prime * result + schema.id.hashCode
  //      result = prime * result + cohorts.map(e => e.id).hashCode
  //      result = prime * result + known.id.hashCode
  //      result
  //  }
  //}
  //
  //final case class SchemaModelCohortKnown(
  //    schema: ConfigSchema,
  //    model: ConfigModel,
  //    cohorts: Seq[ConfigCohort],
  //    known: ConfigKnown) {
  //  def canEqual(a: Any) = a.isInstanceOf[SchemaModelCohortKnown]
  //  override def equals(that: Any): Boolean = that match {
  //    case that: SchemaModelCohortKnown => that.canEqual(this) && this.hashCode == that.hashCode
  //    case _ => false
  //  }
  //  override def hashCode: Int = {
  //      val prime = 31
  //      var result = 1
  //      result = prime * result + schema.id.hashCode
  //      result = prime * result + model.id.hashCode
  //      result = prime * result + cohorts.map(e => e.id).hashCode
  //      result = prime * result + known.id.hashCode
  //      result
  //  }
  //}
  //
  //final case class SchemaMetaCohortKnown(
  //    schema: ConfigSchema,
  //    meta: ConfigMeta,
  //    cohorts: Seq[ConfigCohort],
  //    known: ConfigKnown) {
  //  def canEqual(a: Any) = a.isInstanceOf[SchemaMetaCohortKnown]
  //  override def equals(that: Any): Boolean = that match {
  //    case that: SchemaMetaCohortKnown => that.canEqual(this) && this.hashCode == that.hashCode
  //    case _ => false
  //  }
  //  override def hashCode: Int = {
  //      val prime = 31
  //      var result = 1
  //      result = prime * result + schema.id.hashCode
  //      result = prime * result + meta.id.hashCode
  //      result = prime * result + cohorts.map(e => e.id).hashCode
  //      result = prime * result + known.id.hashCode
  //      result
  //  }
  //}
  //
  //final case class SchemaModelMetaCohortKnown(
  //    schema: ConfigSchema,
  //    model: ConfigModel,
  //    meta: ConfigMeta,
  //    cohorts: Seq[ConfigCohort],
  //    known: ConfigKnown) {
  //  def canEqual(a: Any) = a.isInstanceOf[SchemaModelMetaCohortKnown]
  //  override def equals(that: Any): Boolean = that match {
  //    case that: SchemaModelMetaCohortKnown => that.canEqual(this) && this.hashCode == that.hashCode
  //    case _ => false
  //  }
  //  override def hashCode: Int = {
  //      val prime = 31
  //      var result = 1
  //      result = prime * result + schema.id.hashCode
  //      result = prime * result + model.id.hashCode
  //      result = prime * result + meta.id.hashCode
  //      result = prime * result + cohorts.map(e => e.id).hashCode
  //      result = prime * result + known.id.hashCode
  //      result
  //  }
  //}
  //
  //val schemaModelCohortKnowns: Seq[SchemaModelCohortKnown] = {
  //  val x = for {
  //    schema <- projectConfig.Schemas
  //    model <- projectConfig.Models.filter(e => ! e.metas.isDefined && e.knowns.isDefined && schema.models.contains(e.id))
  //    known <- projectConfig.Knowns.filter(e => model.knowns.get.contains(e.id))
  //  } yield {
  //    model.design match {
  //      case "full" =>
  //        Seq(SchemaModelCohortKnown(
  //          schema = schema,
  //          model = model,
  //          cohorts = projectConfig.Cohorts.filter(e => model.cohorts.contains(e.id)),
  //          known = known
  //        ))
  //      case "strat" =>
  //        for {
  //          cohort <- projectConfig.Cohorts.filter(e => model.cohorts.contains(e.id))
  //        } yield {
  //          SchemaModelCohortKnown(
  //            schema = schema,
  //            model = model,
  //            cohorts = Seq(cohort),
  //            known = known
  //          )
  //        }
  //    }
  //  }
  //  val y = for {
  //    schema <- projectConfig.Schemas
  //    model <- projectConfig.Models.filter(e => e.metas.isDefined && e.design == "strat" && e.knowns.isDefined && schema.models.contains(e.id))
  //  } yield {
  //    val metaCohorts = for {
  //      meta <- projectConfig.Metas.filter(e => model.metas.get.contains(e.id))
  //      cohort <- projectConfig.Cohorts.filter(e => meta.cohorts.contains(e.id))
  //    } yield {
  //      cohort
  //    }
  //    for {
  //      cohort <- projectConfig.Cohorts.filter(e => model.cohorts.contains(e.id)) diff metaCohorts
  //      known <- projectConfig.Knowns.filter(e => model.knowns.get.contains(e.id))
  //    } yield {
  //      SchemaModelCohortKnown(
  //        schema = schema,
  //        model = model,
  //        cohorts = Seq(cohort),
  //        known = known
  //      )
  //    }
  //  }
  //  (x ++ y).flatten
  //}.distinct
  //
  //val schemaModelMetaCohortKnowns: Seq[SchemaModelMetaCohortKnown] = {
  //  for {
  //    schema <- projectConfig.Schemas
  //    model <- projectConfig.Models.filter(e => e.metas.isDefined && e.knowns.isDefined && e.design == "strat" && schema.models.contains(e.id))
  //    meta <- projectConfig.Metas.filter(e => model.metas.get.contains(e.id))
  //    known <- projectConfig.Knowns.filter(e => model.knowns.get.contains(e.id))
  //    cohort <- projectConfig.Cohorts.filter(e => meta.cohorts.contains(e.id))
  //  } yield {
  //    SchemaModelMetaCohortKnown(
  //      schema = schema,
  //      model = model,
  //      meta = meta,
  //      cohorts = Seq(cohort),
  //      known = known
  //    )
  //  }
  //}.distinct

}
