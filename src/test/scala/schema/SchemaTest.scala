package schema

import org.scalatest.FunSuite

class SchemaTest extends FunSuite {

  test("check some attributes in source") {
    assert(PetaSkySchema.sourceS(0) == "sourceId")
    assert(PetaSkySchema.sourceS(3) == "objectId")
    assert(PetaSkySchema.sourceS(106) == "flagForWcs")
  }

  test("check some attributes in object") {
    assert(PetaSkySchema.objectS(0) == "objectId")
    assert(PetaSkySchema.objectS(23) == "extendedness")
    assert(PetaSkySchema.objectS(228) == "subChunkId")
  }

  test("check schema lengths") {
    assert(PetaSkySchema.sourceS.length == 107)
    assert(PetaSkySchema.objectS.length == 229)
  }

}
