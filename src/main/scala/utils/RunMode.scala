package utils

object RunMode extends Enumeration{
  type RunMode = Value
  val PRODUCTION, UNIT_TEST = Value
}