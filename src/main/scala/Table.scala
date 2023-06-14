import util.Util.Row
import util.Util.Line
import TestTables.tableImperative
import TestTables.tableFunctional
import TestTables.tableObjectOriented

import javax.print.attribute.standard.MediaSize.Other
import scala.collection.immutable.ListMap

trait FilterCond {
  def &&(other: FilterCond): FilterCond = And(other, this)

  def ||(other: FilterCond): FilterCond = Or(other, this)

  // fails if the column name is not present in the row
  def eval(r: Row): Option[Boolean]
}


case class Field(colName: String, predicate: String => Boolean) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = {
    r.get(colName) match {
      case None => None
      case Some(x) => Some(predicate(x))
    }
  }
}

/*
And by aplying conditions f1 and f2 on table rows
 */

case class And(f1: FilterCond, f2: FilterCond) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = {
    (f1.eval(r), f2.eval(r)) match {
      case (Some(a), Some(b)) => Some(a && b)
      case _ => None
    }
  }
}

/*
Or by aplying conditions f1 and f2 on table rows
 */
case class Or(f1: FilterCond, f2: FilterCond) extends FilterCond {
  // 2.2.
  override def eval(r: Row): Option[Boolean] = {
    (f1.eval(r), f2.eval(r)) match {
      case (Some(a), Some(b)) => Some(a || b)
      case _ => None
    }
  }
}

trait Query {
  def eval: Option[Table]
}

/*
  Atom query which evaluates to the input table
  Always succeeds
 */
case class Value(t: Table) extends Query {
  override def eval: Option[Table] = Some(t)
}

/*
  Selects certain columns from the result of a target query
  Fails with None if some rows are not present in the resulting table
 */
case class Select(columns: Line, target: Query) extends Query {
  override def eval: Option[Table] =
    target.eval match {
      case None => None
      case Some(element) => element.select(columns)
    }
}

/*
  Filters rows from the result of the target query
  Success depends only on the success of the target
 */
case class Filter(condition: FilterCond, target: Query) extends Query {
  override def eval: Option[Table] =
    target.eval match {
      case None => None
      case Some(x) => x.filter(condition)
    }
}

/*
  Creates a new column with default values
  Success depends only on the success of the target
 */
case class NewCol(name: String, defaultVal: String, target: Query) extends Query {
  override def eval: Option[Table] =
    target.eval match {
      case None => None
      case Some(element) => Some(element.newCol(name, defaultVal))
    }
}

/*
  Combines two tables based on a common key
  Success depends on whether the key exists in both tables or not AND on the success of the target
 */
case class Merge(key: String, t1: Query, t2: Query) extends Query {
  override def eval: Option[Table] =
    (t1.eval, t2.eval) match {
      case (Some(table1), Some(table2)) => table1.merge(key, table2)
      case _ => None
    }
}


class Table(columnNames: Line, tabular: List[List[String]]) {
  def getColumnNames: Line = columnNames

  def getTabular: List[List[String]] = tabular

  /*
  Table to cvs
   */
  // 1.1
  override def toString: String = {
    columnNames.mkString(",") ++ "\n" ++ tabular.map(_.mkString(",")).mkString("\n")
  }

  /*
   Selects columns from table
   */
  // 2.1
  def select(columns: Line): Option[Table] = {
    val tabSelect = (columnNames zip tabular.transpose).filter((col) => columns.contains(col._1)).
      map(x => List(x._2)).flatten.transpose
    if (tabSelect.isEmpty) None
    else Option(new Table(columns, tabSelect))
  }

  /*
    Filters table elements which satisfy the condition
   */
  // 2.2
  def filter(cond: FilterCond): Option[Table] = {
    /*
    Constructs rows
     */
    val rows: List[Map[String, String]] = tabular.map(columnNames zip _).map(x => ListMap(x: _*))
    /*
     Filters row list
     */
    val fTable = rows.filter(row =>
      cond.eval(row) match {
        case Some(true) => true
        case _ => false

      }
    )
    /*
    Checks for result. If no elements found returns None
    */
    if (fTable.isEmpty) None
    else {
      val newTabular = fTable.flatten.map(x => List(x._2)).flatten.grouped(columnNames.size).toList
      Option(new Table(columnNames, newTabular))
    }
  }


  // 2.3.
  def newCol(name: String, defaultVal: String): Table = {
    new Table(columnNames :+ name, tabular.map(x => x :+ defaultVal));
  }

  /*
  merges tables by the given column(key)
   */
  // 2.4.
  def merge(key: String, other: Table): Option[Table] = {
    if (!(columnNames.contains(key) && other.getColumnNames.contains(key))) None
    else {
      //column names first table
      val otherTabular: List[List[String]] = other.getTabular
      //column names second table
      val otherColNames: List[String] = other.getColumnNames
      //index key elem first table
      val indexKey = columnNames.indexOf("Language")
      //index key elem second table
      val otherIndexKey = otherColNames.indexOf("Language")
      //merged table columns
      val newColumns: List[String] = columnNames ++ otherColNames.filter(!columnNames.contains(_))
      //columns specific to second table
      val uniColTab2: List[String] = otherColNames.filter(!columnNames.contains(_))
      //keys first table
      val keyElemTab = tabular.map(_.apply(indexKey))
      //keys second table
      val keyElemOther = otherTabular.map(_.apply(otherIndexKey))
      //common keys
      val comKeys = keyElemOther.filter(keyElemTab.contains(_))
      //different keys(second table)
      val diffKeys = keyElemOther.filter(!keyElemTab.contains(_))

      /*
      finds common elements and groups them
       */
      val ans:List[List[String]] = tabular.flatMap(x => otherTabular.flatMap(y => {

        //verifies equality of elements of key column
        if(y.apply(otherIndexKey).equals(x.apply(indexKey))) {

          //combines elements, checks for equality and divides elements from the same position
          List(x.zipWithIndex.flatMap{case (x1,indexX) => y.zipWithIndex.flatMap{case (y1,indexY) =>
            if(!otherColNames.contains(columnNames.apply(indexX)) && indexY == 0) List(x1)
            else if(columnNames.apply(indexX).equals(otherColNames.apply(indexY))) {
              if(y1.equals(x1)) List(x1)
              else List(x1 + ";" + y1)}
            else Nil}})
        }
        else Nil
      }))
      //merges first table rows with previously combined elements
      val combinedTable:List[List[String]] = tabular.zipWithIndex.flatMap{case (x,index) =>
        if((index <= (tabular.length - 1).toInt))
          if(!comKeys.contains(x(indexKey))) List(x)
          else List(ans(comKeys.indexOf(x(indexKey))))
        else Nil
      }
      //constructs rows from second table which are not in the first
      val lastRows = diffKeys.map(x => newColumns.map(y =>
        if(otherColNames.contains(y))
          otherTabular(keyElemOther.indexOf(x))(otherColNames.indexOf(y))
        else ""
      ))
      //constructs last columns of first table
      val lastCol = keyElemTab.map(y => uniColTab2.map(x =>
        if(keyElemOther.contains(y))
          otherTabular(keyElemOther.indexOf(y))(otherColNames.indexOf(x))
        else ""
      ))
      //merges columns with first table
      val halfTable:List[List[String]] = combinedTable.map(x =>
        if(keyElemTab.contains(x(indexKey))) {
          x ++ lastCol(keyElemTab.indexOf(x(indexKey)))
        } else x
      )
      //merges lower and upper parts of the table
      val mergedtable = halfTable ++ lastRows;
      Some(new Table(newColumns,mergedtable))
    }
  }
}
/*
makes table from string
 */
object Table {
  // 1.2
  def apply(s: String): Table = {
    val tableAsList = s.split("\n").map(_.split(",", -1).toList).toList
    new Table(tableAsList.head, tableAsList.tail)
  }
}
