/*
 * Copyright 2026 humnetlab
 *
 * SPDX-License-Identifier: Apache-2.0
 */

package utils

object RunMode extends Enumeration {
  type RunMode = Value
  val PRODUCTION, UNIT_TEST = Value
}
