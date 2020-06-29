package ashwin.gdpr.main

/**
  * This class is used to parse the Input command line Parameters.
  * @author Ashwin Kumar
  */
object CommandLineArgs {
  case class Args(
                   inventory_conf_file_path: String = null
                 )
  def getCommandLineArgs(args: Array[String]): Args = {
    val parser = new scopt.OptionParser[Args]("GenericInventoryProcessor") {

      opt[String]('c', "INVENTORY_CONFIG_FILE")
        .required()
        .action((x, config) => config.copy(inventory_conf_file_path = x))
        .text("Required parameter : INVENTORY CONFIG FILE ")
      help("help") text ("Print this usage message")
    }
    val cmdArgumentsMap = parser.parse(args, Args())
    if (cmdArgumentsMap.size == 0) {
      System.exit(1)
    }
    cmdArgumentsMap.head
  }

}