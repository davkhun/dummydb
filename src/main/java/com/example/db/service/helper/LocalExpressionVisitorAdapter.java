package com.example.db.service.helper;

import com.example.db.model.FoundLine;
import com.example.db.model.SchemaModel;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.ComparisonOperator;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;

@Data
public class LocalExpressionVisitorAdapter extends ExpressionVisitorAdapter {
  private List<FoundLine> result;
  private List<String> error;
  private List<Integer> lines;
  private String tableName;
  private SchemaModel schema;

  public LocalExpressionVisitorAdapter(List<FoundLine> result, List<String> error, SchemaModel schema, String tableName) {
    this.result = result == null? new ArrayList<>(): result;
    this.error = error == null? new ArrayList<>(): error;
    this.schema = schema;
    this.tableName = tableName;
    this.lines = lines == null? new ArrayList<>(): lines;
  }

  @Override
  protected void visitBinaryExpression(BinaryExpression expr) {
    if (expr instanceof ComparisonOperator) {
      String column = expr.getLeftExpression().toString();
      String operator = expr.getStringExpression();
      String value = expr.getRightExpression().toString().replace("'", "");
      // check column in where statement exists
      if (schema.getColumns().stream().filter(x->x.getName().equals(column)).findAny().isEmpty() && !column.equals("_id")) {
        error.add("Column " + expr.getLeftExpression() + " does not exists");
      }
      else  {
        if (column.equals("_id")) {
          result.addAll(DbHelper.getLinesByPk(tableName, TypeHelper.toInt(value), operator));
        }
        else {
          result.addAll(DbHelper.selectFromTableByOneExpression(tableName, column, value, operator));
        }
      }
    }
    super.visitBinaryExpression(expr);
  }

  @Override
  public void visit(LikeExpression expr) {
    String column = expr.getLeftExpression().toString();
    String operator;
    String value = expr.getRightExpression().toString().replace("'", "");
    if (value.startsWith("%") && value.endsWith("%")) {
      operator = "likeFull";
    }
    else {
      if (value.startsWith("%")) {
        operator = "likeLeft";
      }
      else if (value.endsWith("%")) {
        operator = "likeRight";
      }
      // if using like without any '%' its just equals
      else {
        operator = "=";
      }
    }
    value = value.replace("%", "");
    result.addAll(DbHelper.selectFromTableByOneExpression(tableName, column, value, operator));
    super.visit(expr);
  }

  // very simple OR statement by 2 expressions
  @Override
  public void visit(OrExpression expr) {
    Expression left = expr.getLeftExpression();
    Expression right = expr.getRightExpression();
    if (left.toString().toLowerCase().contains("like")) {
      visit((LikeExpression) left);
    }
    else {
      visitBinaryExpression((BinaryExpression) left);
    }

    if (right.toString().toLowerCase().contains("like")) {
      visit((LikeExpression) right);
    }
    else {
      visitBinaryExpression((BinaryExpression) right);
    }
  }

  // very simple AND statement by 2 expressions
  @Override
  public void visit(AndExpression expr) {
    // TODO need optimization for search by _id. First, need to calculate - how much lines should be taken from index
    Expression left = expr.getLeftExpression();
    Expression right = expr.getRightExpression();
    if (left.toString().toLowerCase().contains("like")) {
      visit((LikeExpression) left);
    }
    else {
      visitBinaryExpression((BinaryExpression) left);
    }
    if (right.toString().toLowerCase().contains("like")) {
      String column = ((LikeExpression)right).getLeftExpression().toString();
      String operator;
      String value = ((LikeExpression)right).getRightExpression().toString().replace("'", "");
      if (value.startsWith("%") && value.endsWith("%")) {
        operator = "likeFull";
      }
      else {
        if (value.startsWith("%")) {
          operator = "likeLeft";
        }
        else if (value.endsWith("%")) {
          operator = "likeRight";
        }
        // if using like without any '%' its just equals
        else {
          operator = "=";
        }
      }
      value = value.replace("%", "");
      DbHelper.filterFromTableByOneExpression(tableName, result, column, value, operator);
    }
    else {
      String column = ((BinaryExpression)right).getLeftExpression().toString();
      String operator = ((BinaryExpression)right).getStringExpression();
      String value = ((BinaryExpression)right).getRightExpression().toString().replace("'", "");
      DbHelper.filterFromTableByOneExpression(tableName, result, column, value, operator);
    }
  }
}
