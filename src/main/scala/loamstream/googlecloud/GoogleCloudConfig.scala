package loamstream.googlecloud

import java.nio.file.Path

import scala.util.Try

import com.typesafe.config.Config
import loamstream.conf.ValueReaders

import GoogleCloudConfig.Defaults

/**
 * @author clint
 * Nov 28, 2016
 */
final case class GoogleCloudConfig(
    gcloudBinary: Path,
    projectId: String,
    clusterId: String,
    credentialsFile: Path,
    zone: String = Defaults.zone,
    masterMachineType: String = Defaults.masterMachineType,
    masterBootDiskSize: Int = Defaults.masterBootDiskSize, // in GB
    numWorkers: Int = Defaults.numWorkers, // minimum 2
    workerMachineType: String = Defaults.workerMachineType,
    workerBootDiskSize: Int = Defaults.workerBootDiskSize, // in GB
    imageVersion: String = Defaults.imageVersion, // 2.x not supported by Hail
    scopes: String = Defaults.scopes)
    
object GoogleCloudConfig {
  object Defaults { // for creating a minimal cluster
    val zone: String = "us-central1-f"
    val masterMachineType: String = "n1-standard-1"
    val masterBootDiskSize: Int = 20
    val numWorkers: Int = 2
    val workerMachineType: String ="n1-standard-1"
    val workerBootDiskSize: Int = 20
    val imageVersion: String = "1.0"
    val scopes: String = "https://www.googleapis.com/auth/cloud-platform"
  }
  
  def fromConfig(config: Config): Try[GoogleCloudConfig] = {
    import net.ceedubs.ficus.Ficus._
    import net.ceedubs.ficus.readers.ArbitraryTypeReader._
    import ValueReaders.PathReader
    
    //NB: Ficus now marshals the contents of loamstream.googlecloud into a GoogleCloudConfig instance.
    //Names of fields in GoogleCloudConfig and keys under loamstream.googlecloud must match.
    Try(config.as[GoogleCloudConfig]("loamstream.googlecloud"))
  }
}