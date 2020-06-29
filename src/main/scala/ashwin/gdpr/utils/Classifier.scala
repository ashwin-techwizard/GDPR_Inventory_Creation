package ashwin.gdpr.utils

import org.apache.spark.sql.functions.udf

object Classifier {

  /**
    * This method uses the column name to Classify whether the data is PII (Personally Identifiable Information)
    *
    * @return String (Classification FLAG)
    * @author Ashwin
    */
  def potentialPD = udf((attribute: String, prfxStr: String, sfxStr: String, parentKwStr: String) => {
    // Converting the input attribute to lowercase
    val prfxSet = prfxStr.split(",").toSet[String]
    val sfxSet = sfxStr.split(",").toSet[String]
    val parentKwSet = parentKwStr.split(",").toSet[String]
    val lowAttribute = attribute.toLowerCase
    var ret = "N"

    // 1- Checking for attributes under root - Attribute names tend to be more descriptive
    // We need the attribute to have a prefix and a suffix to be consider a PD

    for (prfx <- prfxSet) {
      if (lowAttribute.contains(prfx)) {
        for (sfx <- sfxSet) {
          if (lowAttribute.contains(sfx)) {
            ret = "Y"
          }
        }
      }
    }
    // 2- Checking for nested attributes i.e. Contact.Number - Attribute name is partially defined by the parent

    for (parKW <- parentKwSet) {
      if (lowAttribute.contains(parKW)) {
        // This means a strong word is present in the path.
        val fromSKW = lowAttribute.substring(lowAttribute.indexOf(parKW))
        // We now check the attribute name (that is last element in the path)
        val idxNextDot = fromSKW.lastIndexOf(".")
        if (idxNextDot > 0) {
          val remainder = fromSKW.substring(idxNextDot)
          for (sfx <- sfxSet) {
            if (remainder.contains(sfx)) {
              ret = "Y"
            }
          }
        }
      }
    }
    ret
  })
}
