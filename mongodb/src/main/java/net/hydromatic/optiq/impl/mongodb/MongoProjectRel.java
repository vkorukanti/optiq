/*
// Licensed to Julian Hyde under one or more contributor license
// agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
//
// Julian Hyde licenses this file to you under the Apache License,
// Version 2.0 (the "License"); you may not use this file except in
// compliance with the License. You may obtain a copy of the License at:
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
*/
package net.hydromatic.optiq.impl.mongodb;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.Pair;

import java.util.*;

/**
 * Implementation of {@link ProjectRel} relational expression in
 * MongoDB.
 */
public class MongoProjectRel extends ProjectRelBase implements MongoRel {
  private final Pair<String, String> op;

  public MongoProjectRel(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode child, List<RexNode> exps, Pair<String, String> op,
      RelDataType rowType, int flags) {
    super(cluster, traitSet, child, exps, rowType, flags);
    this.op = op;
    assert getConvention() == MongoRel.CONVENTION;
    assert getConvention() == child.getConvention();
  }

  @Override public ProjectRelBase copy(RelTraitSet traitSet, RelNode input,
      List<RexNode> exps, RelDataType rowType) {
    return new MongoProjectRel(getCluster(), traitSet, input, exps,
        op, rowType, flags);
  }

  @Override public RelOptCost computeSelfCost(RelOptPlanner planner) {
    return super.computeSelfCost(planner).multiplyBy(0.1);
  }

  public void implement(Implementor implementor) {
    implementor.visitChild(0, getChild());
    implementor.add(op.left, op.right);
  }
}

// End MongoProjectRel.java
