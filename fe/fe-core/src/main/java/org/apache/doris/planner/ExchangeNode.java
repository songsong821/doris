// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/ExchangeNode.java
// and modified by Doris

package org.apache.doris.planner;

import org.apache.doris.analysis.SortInfo;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.analysis.TupleId;
import org.apache.doris.common.Pair;
import org.apache.doris.nereids.glue.translator.PlanTranslatorContext;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeType;
import org.apache.doris.planner.LocalExchangeNode.LocalExchangeTypeRequire;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TExchangeNode;
import org.apache.doris.thrift.TExplainLevel;
import org.apache.doris.thrift.TPartitionType;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPlanNodeType;

import java.util.Collections;

/**
 * Receiver side of a 1:n data stream. Logically, an ExchangeNode consumes the data
 * produced by its children. For each of the sending child nodes the actual data
 * transmission is performed by the DataStreamSink of the PlanFragment housing
 * that child node. Typically, an ExchangeNode only has a single sender child but,
 * e.g., for distributed union queries an ExchangeNode may have one sender child per
 * union operand.
 *
 * If a (optional) SortInfo field is set, the ExchangeNode will merge its
 * inputs on the parameters specified in the SortInfo object. It is assumed that the
 * inputs are also sorted individually on the same SortInfo parameter.
 */
public class ExchangeNode extends PlanNode {

    public static final String EXCHANGE_NODE = "EXCHANGE";
    public static final String MERGING_EXCHANGE_NODE = "MERGING-EXCHANGE";

    // The parameters based on which sorted input streams are merged by this
    // exchange node. Null if this exchange does not merge sorted streams
    private SortInfo mergeInfo;

    private boolean isRightChildOfBroadcastHashJoin = false;
    private TPartitionType partitionType;

    /**
     * use for Nereids only.
     */
    public ExchangeNode(PlanNodeId id, PlanNode inputNode) {
        super(id, inputNode, EXCHANGE_NODE);
        offset = 0;
        limit = -1;
        this.conjuncts = Collections.emptyList();
        children.add(inputNode);
        TupleDescriptor outputTupleDesc = inputNode.getOutputTupleDesc();
        updateTupleIds(outputTupleDesc);
    }

    public TPartitionType getPartitionType() {
        return partitionType;
    }

    public void setPartitionType(TPartitionType partitionType) {
        this.partitionType = partitionType;
    }

    public void updateTupleIds(TupleDescriptor outputTupleDesc) {
        if (outputTupleDesc != null) {
            clearTupleIds();
            tupleIds.add(outputTupleDesc.getId());
        } else {
            clearTupleIds();
            tupleIds.addAll(getChild(0).getOutputTupleIds());
        }
    }

    /**
     * Set the parameters used to merge sorted input streams. This can be called
     * after init().
     */
    public void setMergeInfo(SortInfo info) {
        this.mergeInfo = info;
        this.planNodeName =  "V" + MERGING_EXCHANGE_NODE;
    }

    @Override
    protected void toThrift(TPlanNode msg) {
        msg.setIsSerialOperator(isSerialOperatorOnBe(ConnectContext.get()));
        msg.node_type = TPlanNodeType.EXCHANGE_NODE;
        msg.exchange_node = new TExchangeNode();
        for (TupleId tid : tupleIds) {
            msg.exchange_node.addToInputRowTuples(tid.asInt());
        }
        if (mergeInfo != null) {
            msg.exchange_node.setSortInfo(mergeInfo.toThrift());
        }
        msg.exchange_node.setOffset(offset);
        msg.exchange_node.setPartitionType(partitionType);
    }

    @Override
    public int getNumInstances() {
        return numInstances;
    }

    @Override
    public String getNodeExplainString(String prefix, TExplainLevel detailLevel) {
        return prefix + "offset: " + offset + "\n";
    }

    public boolean isRightChildOfBroadcastHashJoin() {
        return isRightChildOfBroadcastHashJoin;
    }

    public void setRightChildOfBroadcastHashJoin(boolean value) {
        isRightChildOfBroadcastHashJoin = value;
    }

    /**
     * If table `t1` has unique key `k1` and value column `v1`.
     * Now use plan below to load data into `t1`:
     * ```
     * FRAGMENT 0:
     *  Merging Exchange (id = 1)
     *   NL Join (id = 2)
     *  DataStreamSender (id = 3, dst_id = 3) (OLAP_TABLE_SINK_HASH_PARTITIONED)
     *
     * FRAGMENT 1:
     *  Exchange (id = 3)
     *  OlapTableSink (id = 4) ```
     *
     * In this plan, `Exchange (id = 1)` needs to do merge sort using column `k1` and `v1` so parallelism
     * of FRAGMENT 0 must be 1 and data will be shuffled to FRAGMENT 1 which also has only 1 instance
     * because this loading job relies on the global ordering of column `k1` and `v1`.
     *
     * So FRAGMENT 0 should not use serial source.
     */
    @Override
    public boolean isSerialNode() {
        return (ConnectContext.get() != null && ConnectContext.get().getSessionVariable().isUseSerialExchange()
                || partitionType == TPartitionType.UNPARTITIONED) && mergeInfo == null;
    }

    @Override
    public boolean isSerialOperatorOnBe(ConnectContext context) {
        return fragment != null
                && (isSerialNode() || fragment.hasSerialScanNode())
                && fragment.useSerialSource(context);
    }

    @Override
    public boolean hasSerialChildren() {
        return isSerialNode();
    }

    @Override
    public boolean hasSerialScanChildren() {
        return false;
    }

    @Override
    public Pair<PlanNode, LocalExchangeType> enforceAndDeriveLocalExchange(PlanTranslatorContext translatorContext,
            PlanNode parent, LocalExchangeTypeRequire parentRequire) {
        // Must match the BE serial condition in toThrift(): isSerialNode() && useSerialSource().
        // Only insert PASSTHROUGH when the exchange will actually be serial on the BE.
        // Without useSerialSource() check, we'd insert PASSTHROUGH in non-pooling fragments
        // where the exchange has N tasks, corrupting broadcast join data distribution
        // Use the unified isSerialOperatorOnBe() which matches toThrift serial condition.
        if (isSerialOperatorOnBe(ConnectContext.get())) {
            // If there is already a serial ancestor in the same pipeline (e.g., serial NLJ
            // for RIGHT_OUTER/FULL_OUTER join), don't insert any local exchange. The serial
            // ancestor already constrains the pipeline to 1 task. Inserting a PASSTHROUGH LE
            // would create a pipeline boundary where the BE LOCAL_EXCHANGE_NODE handler calls
            // set_num_tasks(_num_instances), overriding the serial constraint and causing a
            // DCHECK crash (serial operator in pipeline with num_tasks > 1).
            if (translatorContext.hasSerialAncestorInPipeline(this)) {
                return Pair.of(this, LocalExchangeType.NOOP);
            }
            // Serial HASH/BUCKET exchange:
            // In pooling fragments, return NOOP so parent inserts hash/bucket LE with
            // PASSTHROUGH fan-out (heavy-ops avoidance). Serial exchange has 1 task,
            // LE fans out to _num_instances tasks.
            // In non-pooling fragments, report the actual distribution type so parent's
            // require is satisfied without inserting LE. The serial exchange reduces
            // pipeline num_tasks to 1, matching BE-native behavior. Inserting LE in
            // non-pooling fragments creates a pipeline split where downstream has
            // _num_instances tasks but only 1 sender, causing shared-state mismatch.
            boolean useSerial = fragment != null
                    && fragment.useSerialSource(translatorContext.getConnectContext());
            if (partitionType == TPartitionType.HASH_PARTITIONED
                    || partitionType == TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED) {
                if (useSerial) {
                    return Pair.of(this, LocalExchangeType.NOOP);
                }
                LocalExchangeType outputType = partitionType == TPartitionType.HASH_PARTITIONED
                        ? LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE
                        : LocalExchangeType.BUCKET_HASH_SHUFFLE;
                return Pair.of(this, outputType);
            }
            // For UNPARTITIONED (broadcast): PASSTHROUGH fan-out is needed because
            // the exchange has 1 task but downstream needs N tasks.
            if (useSerial) {
                PlanNode pt = new LocalExchangeNode(translatorContext.nextPlanNodeId(),
                        this, LocalExchangeType.PASSTHROUGH, null);
                return Pair.of(pt, LocalExchangeType.PASSTHROUGH);
            }
            return Pair.of(this, LocalExchangeType.NOOP);
        } else if (partitionType == TPartitionType.HASH_PARTITIONED) {
            return Pair.of(this, LocalExchangeType.GLOBAL_EXECUTION_HASH_SHUFFLE);
        } else if (partitionType == TPartitionType.BUCKET_SHFFULE_HASH_PARTITIONED) {
            return Pair.of(this, LocalExchangeType.BUCKET_HASH_SHUFFLE);
        } else {
            return Pair.of(this, LocalExchangeType.NOOP);
        }
    }
}
