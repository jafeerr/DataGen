package com.mindtree.datageneration

import com.mindtree.datageneration.MyTest.{list1, list2}

object MyTest extends App {

  var list1 = new ListNode(0)
  //val list12 = new ListNode(5)
  //list1.next = list12
  var list2 = new ListNode(0)
  /*val list21 = new ListNode(8)
  list2.next = list21
*/
  var result = addTwoNumbers(list1, list2)
  printNode(result)
  class ListNode(var _x: Int = 0) {
    var next: ListNode = null
    var x: Int = _x
  }
  def addTwoNumbers(l1: ListNode, l2: ListNode): ListNode = {
    var head: ListNode = null
    var remainder = 0
    var temp: ListNode = null
    var list1: ListNode = l1
    var list2: ListNode = l2

    while (list1 != null || list2 != null) {
      val sum = (if (list1 == null) 0 else list1.x) + (if (list2 == null) 0
                                                       else list2.x) + remainder
      remainder = sum / 10
      val node = new ListNode(sum % 10)

      if (head == null) {
        head = node
      }
      else
      temp.next = node

      temp = node
      list1 = list1.next
      list2 = list2.next
      printNode(list1)
      printNode(list2)
    }
    if (remainder > 0) {
      val rem = new ListNode(remainder)
      temp.next = rem
    }

    head
  }
  def printNode(node: ListNode): Unit = {
    var temp = node
    while (temp != null) {
      println(temp.x)
      temp = temp.next
    }
  }

}
