/*
 * Copyright 2015, The Querydsl Team (http://www.querydsl.com/team)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.egopulse.querydsl.mongodb;

import java.util.Collection;
import java.util.List;
import java.util.regex.Pattern;

import com.mongodb.client.model.Filters;
import org.bson.*;
import org.bson.conversions.Bson;
import com.mongodb.BasicDBObject;
import com.mongodb.DBRef;
import com.querydsl.core.types.*;

/**
 * Serializes the given Querydsl query to a DBObject query for MongoDB
 *
 * @author laimw
 *
 */
public class MongodbSerializer implements Visitor<Object, Void> {

    public Object handle(Expression<?> expression) {
        // Delegate to visit operation only
        return expression.accept(this, null);
    }

    public BasicDBObject toSort(List<OrderSpecifier<?>> orderBys) {
        BasicDBObject sort = new BasicDBObject();
        for (OrderSpecifier<?> orderBy : orderBys) {
            Object key = orderBy.getTarget().accept(this, null);
            sort.append(key.toString(), orderBy.getOrder() == Order.ASC ? 1 : -1);
        }
        return sort;
    }

    @Override
    public Object visit(Constant<?> expr, Void context) {
        if (Enum.class.isAssignableFrom(expr.getType())) {
            @SuppressWarnings("unchecked") //Guarded by previous check
            Constant<? extends Enum<?>> expectedExpr = (Constant<? extends Enum<?>>) expr;
            return expectedExpr.getConstant().name();
        } else {
            return expr.getConstant();
        }
    }

    private String asDBKey(Operation<?> expr, int index) {
        return (String) asDBValue(expr, index);
    }

    private Object asDBValue(Operation<?> expr, int index) {
        return expr.getArg(index).accept(this, null);
    }

    private String regexValue(Operation<?> expr, int index) {
        return Pattern.quote(expr.getArg(index).accept(this, null).toString());
    }

    protected Bson asDBObject(String key, BsonValue value) {
        return new BsonDocument(key, value);
    }

    protected Bson handleEqOperation(Operation<?> expr) {
        if (expr.getArg(0) instanceof Operation) { // user.addresses.size().eq(1)
            return handleEqSizeOperation(expr);
        } else if (expr.getArg(0) instanceof Path) { // user: xyz
            return Filters.eq(asDBKey(expr, 0), asDBValue(expr, 1));
        } else {
            throw new UnsupportedOperationException("Illegal operation " + expr);
        }
    }

    protected Bson handleEqSizeOperation(Operation<?> expr) {
        Operation<?> lhs = (Operation<?>) expr.getArg(0);
        if (lhs.getOperator() == Ops.COL_SIZE || lhs.getOperator() == Ops.ARRAY_SIZE) {
            return Filters.size(asDBKey(lhs, 0), (Integer) asDBValue(expr, 1));
        } else { // Right hand must be number
            throw new UnsupportedOperationException("Illegal operation " + expr);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Object visit(Operation<?> expr, Void context) {
        Operator op = expr.getOperator();

        /**
         * user.firstName.eq("test")
         * user.addresses.size().eq(20)
         */
        if (op == Ops.EQ) {
            return handleEqOperation(expr);
        }

        /**
         * user.firstName.ne("test")
         */
        else if (op == Ops.NE) {
            return Filters.ne(asDBKey(expr, 0), asDBValue(expr, 1));

        }

        /**
         * user.firstName.isEmpty()
          */
        else if (op == Ops.STRING_IS_EMPTY) {
            return Filters.eq(asDBKey(expr, 0), new BsonString(""));
        }

        /**
         * user.firstName.eq("test").and(user.lastName.eq("blah"))
         */
        else if (op == Ops.AND) {
            Bson leftOperation = (Bson) handle(expr.getArg(0));
            Bson rightOperation = (Bson) handle(expr.getArg(1));

            return Filters.and(leftOperation, rightOperation);
        }


        /**
         * user.firstName.not[Operation]
         */
        else if (op == Ops.NOT) {
            //Handle the not's child
            Operation<?> subOperation = (Operation<?>) expr.getArg(0);
            Operator subOp = subOperation.getOperator();
            if (subOp == Ops.IN) {
                return visit(ExpressionUtils.operation(Boolean.class, Ops.NOT_IN, subOperation.getArg(0),
                        subOperation.getArg(1)), context);
            } else {
                Bson arg = (Bson) handle(expr.getArg(0));
                return Filters.not(arg);
            }
        }

        /**
         * user.firstName.eq("test").or(user.firstName.eq("else"))
         */
        else if (op == Ops.OR) {
            Bson leftOperation = (Bson) handle(expr.getArg(0));
            Bson rightOperation = (Bson) handle(expr.getArg(1));
            return Filters.or(leftOperation, rightOperation);
        }

        /**
         * Text matching operations
         */
        else if (op == Ops.STARTS_WITH) {
            return Filters.regex(asDBKey(expr, 0), Pattern.compile("^" + regexValue(expr, 1)));

        }

        else if (op == Ops.STARTS_WITH_IC) {
            return Filters.regex(asDBKey(expr, 0), Pattern.compile("^" + regexValue(expr, 1), Pattern.CASE_INSENSITIVE));

        }

        else if (op == Ops.ENDS_WITH) {
            return Filters.regex(asDBKey(expr, 0), Pattern.compile(regexValue(expr, 1) + "$"));

        }

        else if (op == Ops.ENDS_WITH_IC) {
            return Filters.regex(asDBKey(expr, 0), Pattern.compile(regexValue(expr, 1) + "$", Pattern.CASE_INSENSITIVE));

        }

        else if (op == Ops.EQ_IGNORE_CASE) {
            return Filters.regex(asDBKey(expr, 0), Pattern.compile("^" + regexValue(expr, 1) + "$", Pattern.CASE_INSENSITIVE));

        }

        else if (op == Ops.STRING_CONTAINS) {
            return Filters.regex(asDBKey(expr, 0), Pattern.compile(".*" + regexValue(expr, 1) + ".*"));

        }

        else if (op == Ops.STRING_CONTAINS_IC) {
            return Filters.regex(asDBKey(expr, 0), Pattern.compile(".*" + regexValue(expr, 1) + ".*", Pattern.CASE_INSENSITIVE));

        }

        else if (op == Ops.MATCHES) {
            return Filters.regex(asDBKey(expr, 0), Pattern.compile(asDBValue(expr, 1).toString()));
        }

        else if (op == Ops.MATCHES_IC) {
            return Filters.regex(asDBKey(expr, 0), Pattern.compile(asDBValue(expr, 1).toString(), Pattern.CASE_INSENSITIVE));

        }

        else if (op == Ops.LIKE) {
            String regex = ExpressionUtils.likeToRegex((Expression) expr.getArg(1)).toString();
            return Filters.regex(asDBKey(expr, 0), Pattern.compile(regex));

        }

        else if (op == Ops.BETWEEN) {
            return Filters.and(Filters.gte(asDBKey(expr, 0), asDBValue(expr, 1)), Filters.lte(asDBKey(expr, 0), asDBValue(expr, 2)));
        }

        else if (op == Ops.IN) {
            int constIndex = 0;
            int exprIndex = 1;
            if (expr.getArg(1) instanceof Constant<?>) {
                constIndex = 1;
                exprIndex = 0;
            }
            if (Collection.class.isAssignableFrom(expr.getArg(constIndex).getType())) {
                @SuppressWarnings("unchecked") //guarded by previous check
                Collection<?> values = ((Constant<? extends Collection<?>>) expr.getArg(constIndex)).getConstant();
                return Filters.in(asDBKey(expr, exprIndex), values);
            }

            /**
             * user.firstName.in(user.lastName)
             */

            else {
                throw new UnsupportedOperationException();
//                Path<?> path = (Path<?>) expr.getArg(exprIndex);
//                Constant<?> constant = (Constant<?>) expr.getArg(constIndex);
//                return asDBObject(asDBKey(expr, exprIndex), convert(path, constant));
            }

        }

        else if (op == Ops.NOT_IN) {
            int constIndex = 0;
            int exprIndex = 1;
            if (expr.getArg(1) instanceof Constant<?>) {
                constIndex = 1;
                exprIndex = 0;
            }
            if (Collection.class.isAssignableFrom(expr.getArg(constIndex).getType())) {
                @SuppressWarnings("unchecked") //guarded by previous check
                Collection<?> values = ((Constant<? extends Collection<?>>) expr.getArg(constIndex)).getConstant();
                return Filters.nin(asDBKey(expr, exprIndex), values);
            } else {
                throw new UnsupportedOperationException();
//                Path<?> path = (Path<?>) expr.getArg(exprIndex);
//                Constant<?> constant = (Constant<?>) expr.getArg(constIndex);
//                return asDBObject(asDBKey(expr, exprIndex), asDBObject("$ne", convert(path, constant)));
            }

        }

        else if (op == Ops.COL_IS_EMPTY) {
            String field = asDBKey(expr, 0);
            return Filters.or(Filters.exists(field, false), Filters.size(field, 0));
        }

        else if (op == Ops.LT) {
            return Filters.lt(asDBKey(expr, 0), asDBValue(expr, 1));

        } else if (op == Ops.GT) {
            return Filters.gt(asDBKey(expr, 0), asDBValue(expr, 1));

        } else if (op == Ops.LOE) {
            return Filters.lte(asDBKey(expr, 0), asDBValue(expr, 1));

        } else if (op == Ops.GOE) {
            return Filters.gte(asDBKey(expr, 0), asDBValue(expr, 1));

        } else if (op == Ops.IS_NULL) {
            return Filters.exists(asDBKey(expr, 0), false);

        } else if (op == Ops.IS_NOT_NULL) {
            return Filters.exists(asDBKey(expr, 0), true);

        }

        else if (op == Ops.CONTAINS_KEY) {
            Path<?> path = (Path<?>) expr.getArg(0);
            Expression<?> key = expr.getArg(1);
            return Filters.exists(visit(path, context) + "." + key.toString(), true);

        }
//        else if (op == MongodbOps.NEAR) {
//            return asDBObject(asDBKey(expr, 0), asDBObject("$near", asDBValue(expr, 1)));
//
//        } else if (op == MongodbOps.NEAR_SPHERE) {
//            return asDBObject(asDBKey(expr, 0), asDBObject("$nearSphere", asDBValue(expr, 1)));
//
//        }
//        else if (op == MongodbOps.ELEM_MATCH) {
//            return Filters.elemMatch(asDBKey(expr, 0), asDBValue(expr, 1));
//        }

        throw new UnsupportedOperationException("Illegal operation " + expr);
    }


    protected Object convert(Path<?> property, Constant<?> constant) {
//        if (isReference(property)) {
//            return asReference(constant.getConstant());
//        } else if (isId(property)) {
//            if (isReference(property.getMetadata().getParent())) {
//                return asReferenceKey(property.getMetadata().getParent().getType(), constant.getConstant());
//            } else if (constant.getType().equals(String.class)) {
//                return new ObjectId((String) constant.getConstant());
//            }
//        }
//        return visit(constant, null);
        throw new UnsupportedOperationException();
    }

    protected DBRef asReferenceKey(Class<?> entity, Object id) {
        // TODO override in subclass
        throw new UnsupportedOperationException();
    }

    protected boolean isId(Path<?> arg) {
        // TODO override in subclass
        return false;
    }

    @Override
    public String visit(Path<?> expr, Void context) {
        PathMetadata metadata = expr.getMetadata();
        if (metadata.getParent() != null) {
            Path<?> parent = metadata.getParent();
            if (parent.getMetadata().getPathType() == PathType.DELEGATE) {
                parent = parent.getMetadata().getParent();
            }
            if (metadata.getPathType() == PathType.COLLECTION_ANY) {
                return visit(parent, context);
            } else if (parent.getMetadata().getPathType() != PathType.VARIABLE) {
                String rv = getKeyForPath(expr, metadata);
                String parentStr = visit(parent, context);
                return rv != null ? parentStr + "." + rv : parentStr;
            }
        }
        return getKeyForPath(expr, metadata);
    }

    protected String getKeyForPath(Path<?> expr, PathMetadata metadata) {
        return metadata.getElement().toString();
    }

    /**
     * Unsupported visitors
     */

    @Override
    public Object visit(SubQueryExpression<?> expr, Void context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object visit(ParamExpression<?> expr, Void context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object visit(TemplateExpression<?> expr, Void context) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object visit(FactoryExpression<?> expr, Void context) {
        throw new UnsupportedOperationException();
    }
}
