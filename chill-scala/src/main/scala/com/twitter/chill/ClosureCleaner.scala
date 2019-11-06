/**
 * Copyright (c) 2010, Regents of the University of California.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the University of California, Berkeley nor the
 *       names of its contributors may be used to endorse or promote
 *       products derived from this software without specific prior written
 *       permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package com.twitter.chill

import _root_.java.io.IOException
import _root_.java.lang.reflect.Field
import _root_.java.io.ByteArrayOutputStream
import _root_.java.io.ObjectOutputStream
import _root_.java.lang.invoke.SerializedLambda

import org.apache.xbean.asm7.Opcodes._
import org.apache.xbean.asm7.{ClassReader, ClassVisitor, MethodVisitor, Type}

import scala.annotation.tailrec
import scala.collection.mutable.{Map => MMap, Set => MSet, Stack => MStack}
import scala.language.existentials
import scala.util.Try

object ClosureCleaner {
  private[this] val OUTER = "$outer"

  /** Clean the closure in place. */
  final def apply[T <: AnyRef](func: T): T = {
    try {
      val buffer = new ByteArrayOutputStream
      val oos = new ObjectOutputStream(buffer)
      try {
        oos.writeObject(func.asInstanceOf[Serializable])
      } finally {
        if (oos != null) oos.close()
      }
      buffer.toByteArray
    } catch {
      case _ @(_: IOException | _: ClassCastException) =>
        new TransitiveClosureCleaner(func).clean()
    }
    func
  }

  def isClosure(cls: Class[_]): Boolean =
    cls.getName.contains("$anonfun$")

  private def serializedLambda(closure: AnyRef): Option[SerializedLambda] = {
    val isClosureCandidate =
      closure.getClass.isSynthetic &&
        closure.getClass.getInterfaces.exists(_.getName == "scala.Serializable")

    if (isClosureCandidate) {
      try {
        Option(inspect(closure))
      } catch {
        case e: Exception =>
          None
      }
    } else {
      None
    }
  }

  def inspect(closure: AnyRef): SerializedLambda = {
    val writeReplace = closure.getClass.getDeclaredMethod("writeReplace")
    writeReplace.setAccessible(true)
    writeReplace.invoke(closure).asInstanceOf[SerializedLambda]
  }

  def outerFieldOf(c: Class[_]): Option[Field] =
    Try(c.getDeclaredField(OUTER)).toOption

  def isOuterField(f: Field): Boolean = f.getName == OUTER

  /**
   * Returns the (Class, AnyRef) pairs from highest level to lowest level. The last element is the
   * outer of the closure.
   */
  def outerClassesOf(obj: AnyRef): List[(Class[_], AnyRef)] = {
    @tailrec
    def loop(obj: AnyRef, hierarchy: List[(Class[_], AnyRef)]): List[(Class[_], AnyRef)] =
      outerFieldOf(obj.getClass) match {
        case None    => hierarchy // We have finished
        case Some(f) =>
          // f is the $outer of obj
          f.setAccessible(true)
          // myOuter = obj.$outer
          val myOuter = f.get(obj)
          val outerType = myOuter.getClass

          loop(myOuter, (outerType, myOuter) :: hierarchy)
      }

    loop(obj, Nil)
  }

  def innerClassesOf(func: AnyRef): Set[Class[_]] = {
    val seen = MSet[Class[_]](func.getClass)
    val stack = MStack[Class[_]](func.getClass)
    while (stack.nonEmpty) {
      val cr = AsmUtil.classReader(stack.pop())
      val set = MSet[Class[_]]()
      cr.foreach { reader =>
        reader.accept(new InnerClosureFinder(set), 0)
        (set -- seen).foreach { cls =>
          seen += cls
          stack.push(cls)
        }
      }
    }
    (seen - func.getClass).toSet
  }

  def setOuter(obj: AnyRef, outer: AnyRef): Unit =
    if (outer != null) {
      val field = outerFieldOf(obj.getClass).get
      field.setAccessible(true)
      field.set(obj, outer)
    }

  def copyField(f: Field, old: AnyRef, newv: AnyRef): Unit = {
    f.setAccessible(true)
    val accessedValue = f.get(old)
    f.set(newv, accessedValue)
  }

  def instantiateClass(cls: Class[_]): AnyRef = {
    val objectCtor = classOf[_root_.java.lang.Object].getDeclaredConstructor()

    sun.reflect.ReflectionFactory.getReflectionFactory
      .newConstructorForSerialization(cls, objectCtor)
      .newInstance()
      .asInstanceOf[AnyRef]
  }
}

sealed trait ClosureCleaner {
  import ClosureCleaner._

  /** The closure to clean. */
  val func: AnyRef

  /** Clean [[func]] by replacing its 'outer' with a cleaned clone. See [[cleanOuter]]. */
  def clean(): AnyRef = {
    val newOuter = cleanOuter
    setOuter(func, newOuter)
    func
  }

  /**
   * Create a new 'cleaned' copy of [[func]]'s outer, without modifying the original. The cleaned
   * outer may have null values for fields that are determined to be unneeded in the context
   * of [[func]].
   *
   * @return The cleaned outer.
   */
  def cleanOuter: AnyRef
}

/**
 * An implementation of [[ClosureCleaner]] that cleans [[func]] by transitively tracing its method
 * calls and field references up through its enclosing scopes. A new hierarchy of outers is
 * constructed by cloning the outer scopes and populating only the fields that are to be accessed
 * by [[func]] (including any of its inner closures). Additionally, outers are removed from the
 * new hierarchy if none of their fields are accessed by [[func]].
 */
private final class TransitiveClosureCleaner(val func: AnyRef) extends ClosureCleaner {
  import ClosureCleaner._

  private[this] val outerClasses: List[(Class[_], AnyRef)] = outerClassesOf(func)
  private[this] val accessedFieldsMap: Map[Class[_], Set[Field]] = {
    val accessedFields = outerClasses
      .map(_._1)
      .foldLeft(MMap[Class[_], MSet[String]]()) { (m, cls) =>
        m += ((cls, MSet[String]()))
      }

    (innerClassesOf(func) + func.getClass)
      .foreach {
        AsmUtil.classReader(_).foreach { reader =>
          reader.accept(new AccessedFieldsVisitor(accessedFields), 0)
        }
      }

    accessedFields.iterator.map {
      case (cls, mset) =>
        def toF(ss: Set[String]): Set[Field] = ss.map(cls.getDeclaredField)
        val set = mset.toSet
        (cls, toF(set))
    }.toMap
  }

  override def cleanOuter: AnyRef =
    outerClasses.foldLeft(null: AnyRef) { (prevOuter, clsData) =>
      val (thisOuterCls, realOuter) = clsData
      val nextOuter = instantiateClass(thisOuterCls)
      accessedFieldsMap(thisOuterCls).foreach(
        copyField(_, realOuter, nextOuter)
      )
      /* If this object's outer is not transitively referenced from the starting closure
         (or any of its inner closures), we can null it out. */
      val parent = if (!accessedFieldsMap(thisOuterCls).exists(isOuterField)) {
        null
      } else {
        prevOuter
      }
      setOuter(nextOuter, parent)
      nextOuter
    }
}

private final case class MethodIdentifier[T](cls: Class[T], name: String, desc: String)

private final class AccessedFieldsVisitor(
    output: MMap[Class[_], MSet[String]],
    specificMethod: Option[MethodIdentifier[_]] = None,
    visitedMethods: MSet[MethodIdentifier[_]] = MSet.empty
) extends ClassVisitor(ASM7) {
  override def visitMethod(
      access: Int,
      name: String,
      desc: String,
      sig: String,
      exceptions: Array[String]
  ): MethodVisitor =
    if (specificMethod.isDefined &&
        (specificMethod.get.name != name || specificMethod.get.desc != desc)) {
      null
    } else {
      new MethodVisitor(ASM7) {
        override def visitFieldInsn(op: Int, owner: String, name: String, desc: String): Unit =
          if (op == GETFIELD) {
            val ownerName = owner.replace('/', '.')
            output.keys.iterator
              .filter(_.getName == ownerName)
              .foreach(cl => output(cl) += name)
          }

        override def visitMethodInsn(
            op: Int,
            owner: String,
            name: String,
            desc: String,
            itf: Boolean
        ): Unit = {
          val ownerName = owner.replace('/', '.')
          output.keys.iterator.filter(_.getName == ownerName).foreach { cl =>
            // Check for calls a getter method for a variable in an interpreter wrapper object.
            // This means that the corresponding field will be accessed, so we should save it.
            if (op == INVOKEVIRTUAL && owner.endsWith("$iwC") && !name.endsWith(
                  "$outer"
                )) {
              output(cl) += name
            }
            val m = MethodIdentifier(cl, name, desc)
            if (!visitedMethods.contains(m)) {
              // Keep track of visited methods to avoid potential infinite cycles
              visitedMethods += m
              AsmUtil.classReader(cl).foreach { reader =>
                reader.accept(
                  new AccessedFieldsVisitor(output, Some(m), visitedMethods),
                  0
                )
              }
            }
          }
        }
      }
    }
}

private final class InnerClosureFinder(output: MSet[Class[_]]) extends ClassVisitor(ASM7) {
  private[this] var myName: String = _

  override def visit(
      version: Int,
      access: Int,
      name: String,
      sig: String,
      superName: String,
      interfaces: Array[String]
  ): Unit =
    myName = name

  override def visitMethod(
      access: Int,
      name: String,
      desc: String,
      sig: String,
      exceptions: Array[String]
  ): MethodVisitor =
    new MethodVisitor(ASM7) {
      override def visitMethodInsn(op: Int, owner: String, name: String, desc: String, itf: Boolean): Unit = {
        val argTypes = Type.getArgumentTypes(desc)
        if (op == INVOKESPECIAL && name == "<init>" && argTypes.nonEmpty
            && argTypes(0).toString.startsWith("L")
            && argTypes(0).getInternalName == myName) {
          output += Class.forName(
            owner.replace('/', '.'),
            false,
            Thread.currentThread.getContextClassLoader
          )
        }
      }
    }
}

private object AsmUtil {
  def classReader(cls: Class[_]): Option[ClassReader] = {
    val className = cls.getName.replaceFirst("^.*\\.", "") + ".class"
    Try(new ClassReader(cls.getResourceAsStream(className))).toOption
  }
}
