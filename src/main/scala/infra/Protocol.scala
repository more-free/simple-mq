package infra

object Protocol {
  case class BadItemException() extends RuntimeException

  val QITEM_HEADER_SIZE = 13 // bytes
  val OP_ADD = 0.toByte
  val OP_POP = 1.toByte
}
