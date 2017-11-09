package schema

object PetaSkySchema {

  val sourceS = Utils.loadSchema(getClass.getResource("/Source.sql"))
  val objectS = Utils.loadSchema(getClass.getResource("/Object.sql"))

  val s_objectId = sourceS.indexOf("objectId")
  val s_sourceId = sourceS.indexOf("sourceId")
  val s_ra = sourceS.indexOf("ra")
  val s_decl = sourceS.indexOf("decl")
}
