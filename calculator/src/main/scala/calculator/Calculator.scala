package calculator

sealed abstract class Expr
final case class Literal(v: Double) extends Expr
final case class Ref(name: String) extends Expr
final case class Plus(a: Expr, b: Expr) extends Expr
final case class Minus(a: Expr, b: Expr) extends Expr
final case class Times(a: Expr, b: Expr) extends Expr
final case class Divide(a: Expr, b: Expr) extends Expr

object Calculator {
  def computeValues(
      namedExpressions: Map[String, Signal[Expr]]): Map[String, Signal[Double]] = {
    namedExpressions.foldLeft(Map[String, Signal[Double]]())
                             {case (m, (str, sig)) => 
                               m.updated(str, Signal(eval(sig(), namedExpressions)))}
  }
  

  def eval(expr: Expr, references: Map[String, Signal[Expr]]): Double = {
    def safeEval(expr: Expr, names : Set[String]) : Double = {
      expr match {
        case Literal(v) => v
        case Ref(name) => {
          if (names.contains(name))  Double.NaN
          else safeEval(getReferenceExpr(name, references), names + name)
        }
        case Plus(a, b) => safeEval(a, names) + safeEval(b, names)
        case Minus(a, b) => safeEval(a, names) - safeEval(b, names)
        case Times(a, b) => safeEval(a, names) * safeEval(b, names)
        case Divide(a, b) => safeEval(a, names) / safeEval(b, names)
      }
    }
    safeEval(expr, Set())
  }

  /** Get the Expr for a referenced variables.
   *  If the variable is not known, returns a literal NaN.
   */
  private def getReferenceExpr(name: String,
      references: Map[String, Signal[Expr]]) = {
    references.get(name).fold[Expr] {
      Literal(Double.NaN)
    } { exprSignal =>
      exprSignal()
    }
  }
}
