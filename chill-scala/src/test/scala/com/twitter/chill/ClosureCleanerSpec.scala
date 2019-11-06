/*
Copyright 2012 Twitter, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */

package com.twitter.chill

import org.scalatest._

import org.scalacheck.Arbitrary
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks._

import scala.reflect.ClassTag
import scala.util.Try

class NestedClosuresNotSerializable {
  val irrelevantInt: Int = 1
  def closure(name: String)(body: => Int => Int): Int => Int = body
  def getMapFn: Int => Int = closure("one") {
    def x = irrelevantInt
    def y = 2
    val fn = { a: Int =>
      a + y
    }
    fn
  }
}

class ClosureCleanerSpec extends WordSpec with Matchers with BaseProperties {
  private def debug(x: AnyRef) {
    println(x.getClass)
    println(x.getClass.getDeclaredFields.map { _.toString }.mkString("  "))
  }

  private val someSerializableValue = 1
  private def someSerializableMethod() = 1

  "ClosureCleaner" should {
    "clean basic closures" in {
      assume(!ClosureCleanerSpec.supportsLMFs)
      val closure1 = (x: Int) => someSerializableValue
      val closure2 = (x: Int) => someSerializableMethod()

      law(closure1)
      law(closure2)
    }

    "clean basic nested closures" in {
      assume(!ClosureCleanerSpec.supportsLMFs)
      val closure1 = (i: Int) => {
        Option(i).map { x =>
          x + someSerializableValue
        }
      }
      val closure2 = (j: Int) => {
        Option(j).map { x =>
          x + someSerializableMethod()
        }
      }
      val closure3 = (m: Int) => {
        Option(m).foreach { x =>
          Option(x).foreach { y =>
            Option(y).foreach { z =>
              someSerializableValue
            }
          }
        }
      }

      law(closure1)
      law(closure2)
      law(closure3)
    }

    "clean complex nested closures" in {
      assume(!ClosureCleanerSpec.supportsLMFs)

      law(new NestedClosuresNotSerializable().getMapFn)

      class A1(val f: Int => Int)
      class A2(val f: Int => Int)
      class B extends A1(x => x * x)
      class C extends A2(x => new B().f(x))

      law(new C().f)
    }
  }

  private def isSerializable[T: ClassTag](obj: T): Boolean =
    Try(jdeserialize[T](jserialize(obj.asInstanceOf[Serializable]))).isSuccess

  def law[A: Arbitrary, B](fn: => A => B): Assertion = {
    val fn0 = fn
    assert(!isSerializable(fn0))
    val clean = ClosureCleaner(fn)
    assert(isSerializable(clean))
    forAll { a: A =>
      assert(clean(a) == fn0(a))
    }
  }
}

object ClosureCleanerSpec {
  val supportsLMFs: Boolean = scala.util.Properties.versionString.contains("2.12")
}
