package ashwin.gdpr.entities

/**
  * This is an Object Class that will be utilized to store all the Input Configuration Parameters
  */
 class ConfigParams {
  @scala.beans.BeanProperty var fileType: String = null
  @scala.beans.BeanProperty var satelliteTable: String = null
  @scala.beans.BeanProperty var messageType: String = null
  @scala.beans.BeanProperty var modelledPath: String = null
  @scala.beans.BeanProperty var sourceSystem: String = null
  @scala.beans.BeanProperty var recordsToscan: Integer = null
}
 class ConfigObjects {
  @scala.beans.BeanProperty var files: Array[ConfigParams] = null
  @scala.beans.BeanProperty var outputPath: String = null
  @scala.beans.BeanProperty var delimiter: String = null
  @scala.beans.BeanProperty var prfxSet: String = null
  @scala.beans.BeanProperty var sfxSet: String = null
  @scala.beans.BeanProperty var parAttSet: String = null
}


