/* -------------------------------------------------------------------------
 *
 * nodeIndexscan.cpp
 *	  Routines to support indexed scans of relations
 *
 * Portions Copyright (c) 2020 Huawei Technologies Co.,Ltd.
 * Portions Copyright (c) 1996-2012, PostgreSQL Global Development Group
 * Portions Copyright (c) 1994, Regents of the University of California
 *
 *
 * IDENTIFICATION
 *	  src/gausskernel/runtime/executor/nodeIndexscan.cpp
 *
 * -------------------------------------------------------------------------
 *
 * INTERFACE ROUTINES
 *		ExecIndexScan			scans a relation using an index
 *		IndexNext				retrieve next tuple using index
 *		ExecInitIndexScan		creates and initializes state info.
 *		ExecReScanIndexScan		rescans the indexed relation.
 *		ExecEndIndexScan		releases all storage.
 *		ExecIndexMarkPos		marks scan position.
 *		ExecIndexRestrPos		restores scan position.
 */
#include "postgres.h"
#include "knl/knl_variable.h"
/* "knl"  "Kernel-Native-Language"，内置的扩展，用于在数据库内部实现编程语言的功能，
例如存储过程、触发器、用户定义的函数等。 */
#include "access/nbtree.h"  //"nbtree" 是 B 树索引的实现
#include "access/relscan.h" //relation scan
#include "access/tableam.h" //table access method
#include "catalog/pg_partition_fn.h" //分区键函数（Partitioning Functions）
#include "commands/cluster.h" //集群管理操作
#include "executor/exec/execdebug.h"
#include "executor/node/nodeIndexscan.h"
#include "optimizer/clauses.h"
#include "storage/tcap.h" //Timecapsule 数据的历史追踪和时间旅行查询
#include "utils/array.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h" //relation descriptor 
#include "utils/rel_gs.h"
#include "gstrace/gstrace_infra.h"
#include "gstrace/executer_gstrace.h"
#include "nodes/makefuncs.h"
#include "optimizer/pruning.h"

static TupleTableSlot* ExecIndexScan(PlanState* state);
static TupleTableSlot* IndexNext(IndexScanState* node);
static void ExecInitNextPartitionForIndexScan(IndexScanState* node);

/* ----------------------------------------------------------------
 *		IndexNext
 *
 *		Retrieve a tuple from the IndexScan node's current_relation
 *		using the index specified in the IndexScanState information.
 * ----------------------------------------------------------------
 */
static TupleTableSlot* IndexNext(IndexScanState* node)
{
    EState* estate = NULL;
    ExprContext* econtext = NULL;
    ScanDirection direction;
    IndexScanDesc scandesc;
    HeapTuple tuple;
    TupleTableSlot* slot = NULL;
    bool isUstore = false;

    /*
     * extract necessary information from index scan node
     */
    estate = node->ss.ps.state;
    direction = estate->es_direction;
    /* flip direction 翻转方向if this is an overall backward scan
    BackwardScanDirection = -1,
    NoMovementScanDirection = 0,
    ForwardScanDirection = 1 */
    if (ScanDirectionIsBackward(((IndexScan*)node->ss.ps.plan)->indexorderdir)) {
        if (ScanDirectionIsForward(direction))
            direction = BackwardScanDirection;
        else if (ScanDirectionIsBackward(direction))
            direction = ForwardScanDirection;
    }
    //获取描述符和上下文
    scandesc = node->iss_ScanDesc;
    econtext = node->ss.ps.ps_ExprContext;
    slot = node->ss.ss_ScanTupleSlot;
    //ustore是og的存储引擎
    isUstore = RelationIsUstoreFormat(node->ss.ss_currentRelation);

    /*
     * ok, now that we have what we need, fetch the next tuple.
     */
    while (true) {
        CHECK_FOR_INTERRUPTS();

        IndexScanDesc indexScan = GetIndexScanDesc(scandesc);
        if (isUstore) {
            //xact:transaction 事务
            //node->ss.ps.state->have_current_xact_date:Check whether dirty reads exist in the cursor rollback scenario用于检查在光标回滚场景中是否存在脏读
            if (!IndexGetnextSlot(scandesc, direction, slot, &node->ss.ps.state->have_current_xact_date)) {
                break;
            }
        } else {
            //不是ustore引擎就添加invalid参数的tuple
            if ((tuple = scan_handler_idx_getnext(scandesc, direction, InvalidOid, InvalidBktId,
                                                  &node->ss.ps.state->have_current_xact_date)) == NULL) {
                break;
            }
            /* Update indexScan, because hashbucket may switch current index in scan_handler_idx_getnext */
            indexScan = GetIndexScanDesc(scandesc);
            /*
             * Store the scanned tuple in the scan tuple slot of the scan state.
             * Note: we pass 'false' because tuples returned by amgetnext are
             * pointers onto disk pages and must not be pfree_ext()'d.
             */
            (void)ExecStoreTuple(tuple, /* tuple to store */
                slot,                   /* slot to store in */
                indexScan->xs_cbuf,     /* buffer containing tuple */
                false);                 /* don't pfree */
        }

        /*
         * If the index was lossy(损失), we have to recheck the index quals(限制条件语句) using
         * the fetched tuple.
         */
        if (indexScan->xs_recheck) {
            econtext->ecxt_scantuple = slot;
            ResetExprContext(econtext);
            if (!ExecQual(node->indexqualorig, econtext, false)) {
                /* Fails recheck, so drop it and loop back for another */
                InstrCountFiltered2(node, 1);
                continue;
            }
        }

        return slot;
    }

    /*
     * if we get here it means the index scan failed so we are at the end of
     * the scan..
     */
    return ExecClearTuple(slot);
}

/*
 * IndexRecheck -- access method routine to recheck a tuple in EvalPlanQual
 */
static bool IndexRecheck(IndexScanState* node, TupleTableSlot* slot)
{
    ExprContext* econtext = NULL;

    /*
     * extract necessary information from index scan node
     */
    econtext = node->ss.ps.ps_ExprContext;

    /* Does the tuple meet the indexqual condition? */
    //ecxt:expression context
    econtext->ecxt_scantuple = slot;

    ResetExprContext(econtext);
    //indexqualorig:execution state for indexqualorig(index qual original) expressions
    return ExecQual(node->indexqualorig, econtext, false);//execQual.cpp
    /*ExecQual 函数详解
ExecQual 函数用于评估一个由连接词 AND 组成的布尔表达式（限定符列表），并返回真假值。
如果所有子表达式都不为假，则返回真 (true)。
如果列表为空，也返回真 (true)。
处理 NULL 值
如果一些子表达式返回 NULL 但没有返回假 (false)，则根据三值布尔逻辑，整个连接的结果为 NULL。此时，ExecQual 将返回通过 "resultForNull" 参数指定的值。
调用者在评估 WHERE 子句时应将 "resultForNull" 设置为 FALSE，因为 SQL 规定 WHERE 结果为 NULL 的行不会被选中。
相反，调用者在评估约束条件时应将 "resultForNull" 设置为 TRUE，因为 SQL 也规定 NULL 的约束条件不视为失败。

请注意，该函数不适用于评估布尔表达式的 AND 子句。对于 AND 子句，需要返回 NULL 以便在上层运算符 (例如 ExecEvalAnd 和 ExecEvalOr) 中正确处理。
ExecQual 函数仅适用于评估完整表达式的情况，此时我们可以将 NULL 值视为与其他布尔结果相同。*/
}

/* ----------------------------------------------------------------
 *		ExecIndexScan(node)初始化 IndexScan 算子的状态信息，包括创建表达式上下文、初始化扫描键、
 打开基表和索引表，为执行 IndexScan 算子做好准备，提供必要的环境和数据结构。
 该函数主要用于执行索引扫描操作的初始化工作

 输入PlanState node  :the common abstract superclass for all PlanState-type nodes.
 * ----------------------------------------------------------------
 */
static TupleTableSlot* ExecIndexScan(PlanState* state)
{
    //直接强转指针类型
    IndexScanState* node = castNode(IndexScanState, state);
    /*
     * If we have runtime keys and they've not already been set up, do it now.
     cmpt:compatability postgres type
     */
    if (node->iss_NumRuntimeKeys != 0 && (!node->iss_RuntimeKeysReady || (u_sess->parser_cxt.has_set_uservar && DB_IS_CMPT(B_FORMAT)))) {
        /*
         * set a flag for partitioned table, so we can deal with it specially
         为分区表设置一个标志，这样我们就可以专门处理它
         * when we rescan the partitioned table
         */
        if (node->ss.isPartTbl) {
            if (PointerIsValid(node->ss.partitions)) {
                node->ss.ss_ReScan = true;// 标记为需要重新扫描
                ExecReScan((PlanState*)node);// 执行重新扫描操作
            }
        } else {
            ExecReScan((PlanState*)node);
        }
    }
    //ScanState ss
    //找到IndexScan node的下一个tuple slot并返回
    return ExecScan(&node->ss, (ExecScanAccessMtd)IndexNext, (ExecScanRecheckMtd)IndexRecheck);
}

/* ----------------------------------------------------------------
 *		ExecReScanIndexScan(node)
 *
 *		Recalculates the values of any scan keys whose value depends on
 *		information known at runtime, then rescans the indexed relation.
 *
 *		Updating the scan key was formerly done separately in
 *		ExecUpdateIndexScanKeys. Integrating it into ReScan makes
 *		rescans of indices and relations/general streams more uniform.
 * ----------------------------------------------------------------
 */
void ExecReScanIndexScan(IndexScanState* node)
{
    /*
     * For recursive-stream rescan, if number of RuntimeKeys not euqal zero,
     * just return without rescan.
     *
     * If we are doing runtime key calculations (ie, any of the index key
     * values weren't simple Consts), compute the new key values.  But first,
     * reset the context so we don't leak memory as each outer tuple is
     * scanned.  Note this assumes that we will recalculate *all* runtime keys
     * on each call.
     * 对于递归流式重新扫描，如果 RuntimeKeys 的数量不为零，则直接返回而不重新扫描。
     * 如果我们正在进行运行时键计算（即，任何索引键值不是简单的常量），则计算新的键值。但是
       在计算新键值之前，先重置上下文，以防每次扫描外部元组时内存泄漏。请注意，这假设我们将在每次调用时重新计算所有运行时键。
     */
    if (node->iss_NumRuntimeKeys != 0) {
        if (node->ss.ps.state->es_recursive_next_iteration) {
            node->iss_RuntimeKeysReady = false;
            return;
            /*RuntimeKey 是用于执行查询计划时在运行时计算的关键值。在执行查询期间，某些条件可能无法在查询计划生成时静态确定
            因此需要在运行时计算。这些条件通常包括函数调用、参数值或其他动态信息*/
        }

        ExprContext* econtext = node->iss_RuntimeContext;

        ResetExprContext(econtext);
        ExecIndexEvalRuntimeKeys(econtext, node->iss_RuntimeKeys, node->iss_NumRuntimeKeys);
    }
    node->iss_RuntimeKeysReady = true;

    /*
     * deal with partitioned table
     */
    bool partpruning = !RelationIsSubPartitioned(node->ss.ss_currentRelation) &&
        ENABLE_SQL_BETA_FEATURE(PARTITION_OPFUSION) && list_length(node->ss.partitions) == 1;
    /* if only one partition is scaned in indexscan, we don't need do rescan for partition */
    if (node->ss.isPartTbl && !partpruning) {//处理没有被剪枝分区表
        /*
         * if node->ss.ss_ReScan = true, just do rescaning as non-partitioned
         * table; else switch to next partition for scaning.
         * 如果 node->ss.ss_ReScan = true，只需重新扫描非分区表；否则切换到下一个分区进行扫描。
         */
        if (node->ss.ss_ReScan ||
            (((Scan *)node->ss.ps.plan)->partition_iterator_elimination)) {
            /* reset the rescan flag */
            node->ss.ss_ReScan = false;
        } else {
            if (!PointerIsValid(node->ss.partitions)) {
                /*
                 * give up rescaning the index if there is no partition to scan
                 */
                return;
            }

            /* switch to next partition for scaning */
            Assert(PointerIsValid(node->iss_ScanDesc));
            scan_handler_idx_endscan(node->iss_ScanDesc);
            /*  initialize Scan for the next partition */
            ExecInitNextPartitionForIndexScan(node);//切换到下一个分区进行扫描
            ExecScanReScan(&node->ss);
            return;
        }
    }

    /* reset index scan */
    scan_handler_idx_rescan(
        node->iss_ScanDesc, node->iss_ScanKeys, node->iss_NumScanKeys, node->iss_OrderByKeys, node->iss_NumOrderByKeys);

    ExecScanReScan(&node->ss);
}

/*
 * ExecIndexEvalRuntimeKeys
 *		Evaluate any runtime key values, and update the scankeys.
 */
void ExecIndexEvalRuntimeKeys(ExprContext* econtext, IndexRuntimeKeyInfo* run_time_keys, int num_run_time_keys)
{
    int j;
    MemoryContext old_context;

    /* We want to keep the key values in per-tuple memory */
    /* 我们希望在每个元组内存中保留键值 */
    old_context = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

    for (j = 0; j < num_run_time_keys; j++) {
        ScanKey scan_key = run_time_keys[j].scan_key;
        ExprState* key_expr = run_time_keys[j].key_expr;
        Datum scan_value;
        bool is_null = false;

        /*
         * For each run-time key, extract the run-time expression and evaluate
         * it with respect to the current context.	We then stick the result
         * into the proper scan key.
         *
         * Note: the result of the eval could be a pass-by-ref value that's
         * stored in some outer scan's tuple, not in
         * econtext->ecxt_per_tuple_memory.  We assume that the outer tuple
         * will stay put throughout our scan.  If this is wrong, we could copy
         * the result into our context explicitly, but I think that's not
         * necessary.
         *
         * It's also entirely possible that the result of the eval is a
         * toasted value.  In this case we should forcibly detoast it, to
         * avoid repeat detoastings each time the value is examined by an
         * index support function.
         * 对于每个运行时键，提取运行时表达式并根据当前上下文进行评估。
         * 我们然后将结果放入适当的扫描键中。
         *
         * 注意：eval的结果可能是一个引用传递的值，这个值存储在一些外部扫描的元组中，而不是在
         * econtext->ecxt_per_tuple_memory中。我们假设在我们的扫描过程中，外部元组会保持不变。
         * 如果这个假设是错误的，我们可以显式地将结果复制到我们的上下文中，但我认为这不是必要的。
         *
         * eval的结果也完全可能是一个被压缩的值。在这种情况下，我们应该强制解压它，以避免每次
         * 该值被索引支持函数检查时都重复解压。
         */

        scan_value = ExecEvalExpr(key_expr, econtext, &is_null, NULL);
        if (is_null) {//sk :scan key
        //如果评估的结果是NULL，它将扫描键的参数设置为键值，并设置标志为NULL
            scan_key->sk_argument = scan_value;
            scan_key->sk_flags |= SK_ISNULL;
        } else {
            //如果键值不为NULL，检查键值是否可能被压缩，如果是，强制解压键值
            if (run_time_keys[j].key_toastable)//"toastable"指一个数据项是否可能被"TOAST"（The Oversized-Attribute Storage Technique）处理
                scan_value = PointerGetDatum(PG_DETOAST_DATUM(scan_value));
            scan_key->sk_argument = scan_value;
            scan_key->sk_flags &= ~SK_ISNULL;
        }
    }
       // 切换回旧的内存上下文
    MemoryContextSwitchTo(old_context);
}

/*
 * ExecIndexEvalArrayKeys
 *		Evaluate any array key values, and set up to iterate through arrays.
 *
 * Returns TRUE if there are array elements to consider; FALSE means there
 * is at least one null or empty array, so no match is possible.  On TRUE
 * result, the scankeys are initialized with the first elements of the arrays.
 *	评估任何数组键值，并设置遍历数组。
 * 如果有数组元素需要考虑，则返回TRUE；FALSE表示至少有一个空数组或者空值，所以不可能有匹配。在TRUE的结果下，扫描键会用数组的第一个元素进行初始化。
 */
bool ExecIndexEvalArrayKeys(ExprContext* econtext, IndexArrayKeyInfo* array_keys, int num_array_keys)
{
    bool result = true;
    int j;
    MemoryContext old_context;

    /* We want to keep the arrays in per-tuple memory */
    old_context = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

    for (j = 0; j < num_array_keys; j++) {
        ScanKey scan_key = array_keys[j].scan_key;
        ExprState* array_expr = array_keys[j].array_expr;
        Datum array_datum;
        bool is_null = false;
        ArrayType* array_val = NULL;
        int16 elm_len;
        bool elm_by_val = false;
        char elm_align;
        int num_elems;
        Datum* elem_values = NULL;
        bool* elem_nulls = NULL;

        /*
         * Compute and deconstruct the array expression.计算并解构数组表达式
          (Notes in ExecIndexEvalRuntimeKeys() apply here too.)
         */
        array_datum = ExecEvalExpr(array_expr, econtext, &is_null, NULL);
        if (is_null) {
            result = false;
            break; /* no point in evaluating more */
        }
        array_val = DatumGetArrayTypeP(array_datum);//P :pointer
        /* We could cache this data, but not clear it's worth it */
        get_typlenbyvalalign(ARR_ELEMTYPE(array_val), &elm_len, &elm_by_val, &elm_align);
        deconstruct_array(
            array_val, ARR_ELEMTYPE(array_val), elm_len, elm_by_val, elm_align, &elem_values, &elem_nulls, &num_elems);
        if (num_elems <= 0) {
            result = false;
            break; /* no point in evaluating more */
        }

        /*
         * Note: we expect the previous array data, if any, to be
         * automatically freed by resetting the per-tuple context; hence no
         * pfree's here.
         * 注意：我们期望之前的数组数据（如果有的话）在重置每个元组上下文时自动释放；因此这里没有pfree。
         */
        array_keys[j].elem_values = elem_values;
        array_keys[j].elem_nulls = elem_nulls;
        array_keys[j].num_elems = num_elems;
        scan_key->sk_argument = elem_values[0];
        if (elem_nulls[0])
            scan_key->sk_flags |= SK_ISNULL;
        else
            scan_key->sk_flags &= ~SK_ISNULL;
        array_keys[j].next_elem = 1;
    }

    MemoryContextSwitchTo(old_context);

    return result;
}

/*
 * ExecIndexAdvanceArrayKeys
 *		Advance to the next set of array key values, if any.
 *
 * Returns TRUE if there is another set of values to consider, FALSE if not.
 * On TRUE result, the scankeys are initialized with the next set of values.
 * 前进到下一个数组键值集（如果有的话）。
 * 如果有另一组值需要考虑，则返回TRUE，否则返回FALSE。
 * 如果结果为TRUE，扫描键将用下一组值进行初始化。
 */
bool ExecIndexAdvanceArrayKeys(IndexArrayKeyInfo* array_keys, int num_array_keys)
{
    bool found = false;
    int j;

    /*
     * Note we advance the rightmost array key most quickly, since it will
     * correspond to the lowest-order index column among the available
     * qualifications.	This is hypothesized to result in better locality of
     * access in the index.
     */
    for (j = num_array_keys - 1; j >= 0; j--) {
        ScanKey scan_key = array_keys[j].scan_key;
        int next_elem = array_keys[j].next_elem;
        int num_elems = array_keys[j].num_elems;
        Datum* elem_values = array_keys[j].elem_values;
        bool* elem_nulls = array_keys[j].elem_nulls;

        if (next_elem >= num_elems) {// 如果下一个元素的索引超过了元素数量
            next_elem = 0; // 重置下一个元素的索引为0
            found = false; /* need to advance next array key */
        } else {
            found = true;
        }
        scan_key->sk_argument = elem_values[next_elem];// 设置扫描键的参数为下一个元素的值
        if (elem_nulls[next_elem]) {
            scan_key->sk_flags |= SK_ISNULL;
        } else {
            scan_key->sk_flags &= ~SK_ISNULL;
        }
        array_keys[j].next_elem = next_elem + 1;
        if (found) {
            break;
        }
    }

    return found;
}

/* ----------------------------------------------------------------
 *		ExecEndIndexScan
 * ----------------------------------------------------------------
 */
void ExecEndIndexScan(IndexScanState* node)
{
    Relation index_relation_desc;
    IndexScanDesc index_scan_desc;
    Relation relation;

    /*
     * extract information from the node
     iss:index scan state 从node提取relation和indexscan 描述符
     */
    index_relation_desc = node->iss_RelationDesc;
    index_scan_desc = node->iss_ScanDesc;
    relation = node->ss.ss_currentRelation;

    /*
     * Free the exprcontext(s) ... now dead code, see ExecFreeExprContext
     */
#ifdef NOT_USED
    ExecFreeExprContext(&node->ss.ps);
    if (node->iss_RuntimeContext)
        FreeExprContext(node->iss_RuntimeContext, true);
#endif

    /*
     * clear out tuple table slots
     */
    (void)ExecClearTuple(node->ss.ps.ps_ResultTupleSlot);
    (void)ExecClearTuple(node->ss.ss_ScanTupleSlot);

    /*
     * close the index relation (no-op if we didn't open it)
     */
    if (index_scan_desc)
        scan_handler_idx_endscan(index_scan_desc);

    /*
     * close the index relation (no-op if we didn't open it)
     * close the index relation if the relation is non-partitioned table
     * close the index partitions and table partitions if the relation is
     * non-partitioned table
     */
    if (node->ss.isPartTbl) {//isPartTbl :is partition table
        if (PointerIsValid(node->iss_IndexPartitionList)) {
            Assert(PointerIsValid(index_relation_desc));
            Assert(PointerIsValid(node->ss.partitions));
            Assert(node->ss.partitions->length == node->iss_IndexPartitionList->length);

            Assert(PointerIsValid(node->iss_CurrentIndexPartition));
            releaseDummyRelation(&(node->iss_CurrentIndexPartition));

            Assert(PointerIsValid(node->ss.ss_currentPartition));
            releaseDummyRelation(&(node->ss.ss_currentPartition));

            if (RelationIsSubPartitioned(relation)) {
                releaseSubPartitionList(index_relation_desc, &(node->iss_IndexPartitionList), NoLock);
                releaseSubPartitionList(node->ss.ss_currentRelation, &(node->ss.subpartitions), NoLock);
            } else {
                /* close index partition */
                releasePartitionList(node->iss_RelationDesc, &(node->iss_IndexPartitionList), NoLock);
            }

            /* close table partition */
            releasePartitionList(node->ss.ss_currentRelation, &(node->ss.partitions), NoLock);
        }
    }

    if (index_relation_desc)
        index_close(index_relation_desc, NoLock);

    /*
     * close the heap relation.
     */
    ExecCloseScanRelation(relation);
}

/* ----------------------------------------------------------------
 *		ExecIndexMarkPos
 * ----------------------------------------------------------------
 */
void ExecIndexMarkPos(IndexScanState* node)
{
    scan_handler_idx_markpos(node->iss_ScanDesc);
        /*scan_handler_idx_markpos会看indexRelation是不是包含bucket(检查是否有bucketoid)
    如果是调用index_markpos函数标记当前桶索引扫描位置，否则直接标记scan位置*/
}

/* ----------------------------------------------------------------
 *		ExecIndexRestrPos
 * ----------------------------------------------------------------
 */
void ExecIndexRestrPos(IndexScanState* node)
{
    scan_handler_idx_restrpos(node->iss_ScanDesc);

}

/* ----------------------------------------------------------------
 *		ExecInitIndexScan
 *
 *		Initializes the index scan's state information, creates
 *		scan keys, and opens the base and index relations.
 *
 *		Note: index scans have 2 sets of state information because
 *			  we have to keep track of the base relation and the
 *			  index relation.
 * ----------------------------------------------------------------
 */
void ExecInitIndexRelation(IndexScanState* node, EState* estate, int eflags)
{
    IndexScanState* index_state = node;
    Snapshot scanSnap;
    Relation current_relation = index_state->ss.ss_currentRelation;
    IndexScan *index_scan = (IndexScan *)node->ss.ps.plan;

    /*
     * Choose user-specified snapshot if TimeCapsule clause exists, otherwise 
     * estate->es_snapshot instead.
     * Tv: TimeCapsule version。 创建快照
     */
    scanSnap = TvChooseScanSnap(index_state->iss_RelationDesc, &index_scan->scan, &index_state->ss);

    /* deal with partition info */
    //isPartTbl :is partition table
    if (index_state->ss.isPartTbl) {
        index_state->iss_ScanDesc = NULL;

        if (index_scan->scan.itrs > 0) {
            Partition current_partition = NULL;
            Partition currentindex = NULL;

            /* Initialize table partition list and index partition list for following scan */
            /* 为后续扫描初始化表分区列表和索引分区列表 */
            ExecInitPartitionForIndexScan(index_state, estate);

            if (index_state->ss.partitions != NIL) {
                /* construct a dummy relation with the first table partition for following scan */
                //判断是不是subpartitioned
                if (RelationIsSubPartitioned(current_relation)) {
                    //list_nths:返回list中指定位置的元素
                    Partition subOnePart = (Partition)list_nth(index_state->ss.partitions, index_state->ss.currentSlot);
                    List *currentSubpartList = (List *)list_nth(index_state->ss.subpartitions, 0);//首个子分区
                    List *currentindexlist = (List *)list_nth(index_state->iss_IndexPartitionList, 0);
                    current_partition = (Partition)list_nth(currentSubpartList, 0);
                    currentindex = (Partition)list_nth(currentindexlist, 0);
                    Relation subOnePartRel = partitionGetRelation(index_state->ss.ss_currentRelation, subOnePart);
                    index_state->ss.ss_currentPartition =
                        partitionGetRelation(subOnePartRel, current_partition);
                    releaseDummyRelation(&subOnePartRel);
                } else {
                    current_partition = (Partition)list_nth(index_state->ss.partitions, 0);
                    currentindex = (Partition)list_nth(index_state->iss_IndexPartitionList, 0);
                    index_state->ss.ss_currentPartition =
                        partitionGetRelation(index_state->ss.ss_currentRelation, current_partition);
                }

                index_state->iss_CurrentIndexPartition =
                    partitionGetRelation(index_state->iss_RelationDesc, currentindex);

                /*
                 * Verify if a DDL operation that froze all tuples in the relation
                 * occured after taking the snapshot.
                 */
                if (RelationIsUstoreFormat(index_state->ss.ss_currentPartition)) {
                    //ustroe是og的存储引擎
                    TransactionId relfrozenxid64 = InvalidTransactionId;
                    getPartitionRelxids(index_state->ss.ss_currentPartition, &relfrozenxid64);
                    //判断当前事务和xmax的先后关系
                    if (TransactionIdPrecedes(FirstNormalTransactionId, scanSnap->xmax) &&
                        !TransactionIdIsCurrentTransactionId(relfrozenxid64) &&
                        TransactionIdPrecedes(scanSnap->xmax, relfrozenxid64)) {
                        ereport(ERROR, (errcode(ERRCODE_SNAPSHOT_INVALID),
                            (errmsg("Snapshot too old, IndexRelation is  PartTbl, the info: snapxmax is %lu, "
                                "snapxmin is %lu, csn is %lu, relfrozenxid64 is %lu, globalRecycleXid is %lu.",
                                scanSnap->xmax, scanSnap->xmin, scanSnap->snapshotcsn, relfrozenxid64,
                                g_instance.undo_cxt.globalRecycleXid))));
                    }
                }

                /* Initialize scan descriptor for partitioned table */
                index_state->iss_ScanDesc = scan_handler_idx_beginscan(index_state->ss.ss_currentPartition,
                    index_state->iss_CurrentIndexPartition,
                    scanSnap,
                    index_state->iss_NumScanKeys,
                    index_state->iss_NumOrderByKeys,
                    (ScanState*)index_state);
            }
        }
    } else {
        /*
         * Verify if a DDL operation that froze all tuples in the relation
         * occured after taking the snapshot.
         */
        if (RelationIsUstoreFormat(current_relation)) {
            TransactionId relfrozenxid64 = InvalidTransactionId;
            getRelationRelxids(current_relation, &relfrozenxid64);
            if (TransactionIdPrecedes(FirstNormalTransactionId, scanSnap->xmax) &&
                !TransactionIdIsCurrentTransactionId(relfrozenxid64) &&
                TransactionIdPrecedes(scanSnap->xmax, relfrozenxid64)) {
                ereport(ERROR, (errcode(ERRCODE_SNAPSHOT_INVALID),
                    (errmsg("Snapshot too old, IndexRelation is not  PartTbl, the info: snapxmax is %lu, "
                        "snapxmin is %lu, csn is %lu, relfrozenxid64 is %lu, globalRecycleXid is %lu.",
                        scanSnap->xmax, scanSnap->xmin, scanSnap->snapshotcsn, relfrozenxid64,
                        g_instance.undo_cxt.globalRecycleXid))));
            }
        }

        /*
         * Initialize scan descriptor.
         */
        index_state->iss_ScanDesc = scan_handler_idx_beginscan(current_relation,
            index_state->iss_RelationDesc,
            scanSnap,
            index_state->iss_NumScanKeys,
            index_state->iss_NumOrderByKeys,
            (ScanState*)index_state);
    }

    return;
}

/*
初始化 IndexScan 算子的状态信息
Inputs:
	'node' is the current node of the plan produced by the query planner
 	'estate' is the shared execution state for the plan tree  执行器调用状态
 	'eflags' is a bitwise OR of flag bits described in executor.h  
     exector flag tells the called plan node what to expect. 
@return IndexScanState* 索引扫描状态信息
*/
IndexScanState* ExecInitIndexScan(IndexScan* node, EState* estate, int eflags)
{
    IndexScanState* index_state = NULL;
    Relation current_relation;
    bool relis_target = false;

    gstrace_entry(GS_TRC_ID_ExecInitIndexScan);//生成一个 ENTRY 跟踪记录到内存
    /*
     * create state structure
     */
    index_state = makeNode(IndexScanState);//分配内存
    index_state->ss.ps.plan = (Plan*)node;
    index_state->ss.ps.state = estate;
    index_state->ss.isPartTbl = node->scan.isPartTbl;
    index_state->ss.currentSlot = 0;
    index_state->ss.partScanDirection = node->indexorderdir;
    index_state->ss.ps.ExecProcNode = ExecIndexScan;

    /*
     * Miscellaneous initialization杂项初始化
     *
     * create expression context for node
     */
    ExecAssignExprContext(estate, &index_state->ss.ps);

    index_state->ss.ps.ps_vec_TupFromTlist = false;

    /*
     * initialize child expressions
     *
     * Note: we don't initialize all of the indexqual expression, only the
     * sub-parts corresponding to runtime keys (see below).  Likewise for
     * indexorderby, if any.  But the indexqualorig expression is always
     * initialized even though it will only be used in some uncommon cases ---
     * would be nice to improve that.  (Problem is that any SubPlans present
     * in the expression must be found now...)
     */
   // flt -> flatten,指的是将嵌套的表达式展平成一个单层的列表形式
    if (estate->es_is_flt_frame) {
        index_state->ss.ps.qual = (List*)ExecInitQualByFlatten(node->scan.plan.qual, (PlanState*)index_state);
        index_state->indexqualorig = (List*)ExecInitQualByFlatten(node->indexqualorig, (PlanState*)index_state);
    } else {
        index_state->ss.ps.targetlist = (List*)ExecInitExprByRecursion((Expr*)node->scan.plan.targetlist, (PlanState*)index_state);
        index_state->ss.ps.qual = (List*)ExecInitExprByRecursion((Expr*)node->scan.plan.qual, (PlanState*)index_state);
        index_state->indexqualorig = (List*)ExecInitExprByRecursion((Expr*)node->indexqualorig, (PlanState*)index_state);
    }

    /*
     * open the base relation and acquire appropriate lock on it.获取适当的锁
     ExecOpenScanRelation里面会申请锁
     */
    current_relation = ExecOpenScanRelation(estate, node->scan.scanrelid);

    index_state->ss.ss_currentRelation = current_relation;
    index_state->ss.ss_currentScanDesc = NULL; /* no heap scan here */
    /*
     * tuple table initialization
     */
    ExecInitResultTupleSlot(estate, &index_state->ss.ps, current_relation->rd_tam_ops);
    ExecInitScanTupleSlot(estate, &index_state->ss, current_relation->rd_tam_ops);

    /*
     * get the scan type from the relation descriptor.
     */
    ExecAssignScanType(&index_state->ss, CreateTupleDescCopy(RelationGetDescr(current_relation)));
    index_state->ss.ss_ScanTupleSlot->tts_tupleDescriptor->td_tam_ops = current_relation->rd_tam_ops;

    /*
     * Initialize result tuple type and projection info.初始化结果元组类型和投影信息
     TL：目标列表TargetList
     定义有两个参数1.planstate 
     2.TableAmRoutine=TableAmHeap，是在executor.h里面传入的。 AM：accessor methods
     */
    ExecAssignResultTypeFromTL(&index_state->ss.ps);
    //tts:TupleTableSlot  ops -> operations
    index_state->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor->td_tam_ops =
            index_state->ss.ss_ScanTupleSlot->tts_tupleDescriptor->td_tam_ops;

    ExecAssignScanProjectionInfo(&index_state->ss);

    Assert(index_state->ss.ps.ps_ResultTupleSlot->tts_tupleDescriptor->td_tam_ops);

    /*
     * If we are just doing EXPLAIN (ie, aren't going to run the plan), stop
     * here.  This allows an index-advisor plugin to EXPLAIN a plan containing
     * references to nonexistent indexes.
     */
    if (eflags & EXEC_FLAG_EXPLAIN_ONLY) {
        gstrace_exit(GS_TRC_ID_ExecInitIndexScan);
        return index_state;
    }

    /*
     * Open the index relation.
     *
     * If the parent table is one of the target relations of the query, then
     * InitPlan already opened and write-locked the index, so we can avoid
     * taking another lock here.  Otherwise we need a normal reader's lock.
     * 如果父表是查询的目标关系之一，则 InitPlan 已经打开并对索引进行了写锁定，
     * 因此我们可以避免在这里再次获取锁。否则，我们需要正常的读取锁。
     * Desc:desciptor 描述符
     */
    relis_target = ExecRelationIsTargetRelation(estate, node->scan.scanrelid);
    index_state->iss_RelationDesc = index_open(node->indexid, relis_target ? NoLock : AccessShareLock);
    if (!IndexIsUsable(index_state->iss_RelationDesc->rd_index)) {
        ereport(ERROR,
            (errcode(ERRCODE_INDEX_CORRUPTED),
                errmsg("can't initialize index scans using unusable index \"%s\"",
                    RelationGetRelationName(index_state->iss_RelationDesc))));
    }
    /*
     * Initialize index-specific scan state
     */
    index_state->iss_RuntimeKeysReady = false;
    index_state->iss_RuntimeKeys = NULL;
    index_state->iss_NumRuntimeKeys = 0;

    /*
     * build the index scan keys from the index qualification 从索引限制条件构建索引扫描键
     qual:查询中的限定条件或过滤条件。这些条件用于过滤出符合特定条件的行或元组。通常通过 WHERE 子句来定义
     */
    ExecIndexBuildScanKeys((PlanState*)index_state,
        index_state->iss_RelationDesc,
        node->indexqual,
        false,
        &index_state->iss_ScanKeys,
        &index_state->iss_NumScanKeys,
        &index_state->iss_RuntimeKeys,
        &index_state->iss_NumRuntimeKeys,
        NULL, /* no ArrayKeys */
        NULL);

    /*
     * any ORDER BY exprs have to be turned into scankeys in the same way
     */
    ExecIndexBuildScanKeys((PlanState*)index_state,
        index_state->iss_RelationDesc,
        node->indexorderby,
        true,
        &index_state->iss_OrderByKeys,
        &index_state->iss_NumOrderByKeys,
        &index_state->iss_RuntimeKeys,
        &index_state->iss_NumRuntimeKeys,
        NULL, /* no ArrayKeys */
        NULL);

    /*
     * If we have runtime keys, we need an ExprContext to evaluate them. The
     * node's standard context won't do because we want to reset that context
     * for every tuple.  So, build another context just like the other one...
     * -tgl 7/11/00
     */
    if (index_state->iss_NumRuntimeKeys != 0) {
        ExprContext* stdecontext = index_state->ss.ps.ps_ExprContext;

        ExecAssignExprContext(estate, &index_state->ss.ps);
        index_state->iss_RuntimeContext = index_state->ss.ps.ps_ExprContext;
        index_state->ss.ps.ps_ExprContext = stdecontext;
    } else {
        index_state->iss_RuntimeContext = NULL;
    }

    /* deal with partition info */
    ExecInitIndexRelation(index_state, estate, eflags);

    /*
     * If no run-time keys to calculate, go ahead and pass the scankeys to the
     * index AM.
     */
    if (index_state->iss_ScanDesc == NULL) {
        index_state->ss.ps.stubType = PST_Scan;
    } else if (index_state->iss_NumRuntimeKeys == 0) {
        scan_handler_idx_rescan_local(index_state->iss_ScanDesc,
            index_state->iss_ScanKeys,
            index_state->iss_NumScanKeys,
            index_state->iss_OrderByKeys,
            index_state->iss_NumOrderByKeys);
    }

    /*
     * all done.
     */
    gstrace_exit(GS_TRC_ID_ExecInitIndexScan);
    return index_state;
}

/*
 * ExecIndexBuildScanKeys
 *		Build the index scan keys from the index qualification expressions
 *
 * The index quals are passed to the index AM in the form of a ScanKey array.
 * This routine sets up the ScanKeys, fills in all constant fields of the
 * ScanKeys, and prepares information about the keys that have non-constant
 * comparison values.  We divide index qual expressions into five types:
 *
 * 1. Simple operator with constant comparison value ("indexkey op constant").
 * For these, we just fill in a ScanKey containing the constant value.
 *
 * 2. Simple operator with non-constant value ("indexkey op expression").
 * For these, we create a ScanKey with everything filled in except the
 * expression value, and set up an IndexRuntimeKeyInfo struct to drive
 * evaluation of the expression at the right times.
 *
 * 3. RowCompareExpr ("(indexkey, indexkey, ...) op (expr, expr, ...)").
 * For these, we create a header ScanKey plus a subsidiary ScanKey array,
 * as specified in access/skey.h.  The elements of the row comparison
 * can have either constant or non-constant comparison values.
 *
 * 4. ScalarArrayOpExpr ("indexkey op ANY (array-expression)").  If the index
 * has rd_am->amsearcharray, we handle these the same as simple operators,
 * setting the SK_SEARCHARRAY flag to tell the AM to handle them.  Otherwise,
 * we create a ScanKey with everything filled in except the comparison value,
 * and set up an IndexArrayKeyInfo struct to drive processing of the qual.
 * (Note that if we use an IndexArrayKeyInfo struct, the array expression is
 * always treated as requiring runtime evaluation, even if it's a constant.)
 *
 * 5. NullTest ("indexkey IS NULL/IS NOT NULL").  We just fill in the
 * ScanKey properly.
 *
 * This code is also used to prepare ORDER BY expressions for amcanorderbyop
 * indexes.  The behavior is exactly the same, except that we have to look up
 * the operator differently.  Note that only cases 1 and 2 are currently
 * possible for ORDER BY.
 *
 * Input params are:
 *
 * plan_state: executor state node we are working for
 * index: the index we are building scan keys for
 * quals: indexquals (or indexorderbys) expressions
 * is_order_by: true if processing ORDER BY exprs, false if processing quals
 * *run_time_keys: ptr to pre-existing IndexRuntimeKeyInfos, or NULL if none
 * *num_run_time_keys: number of pre-existing runtime keys
 *
 * Output params are:
 *
 * *scan_keys: receives ptr to array of ScanKeys
 * *num_scan_keys: receives number of scankeys
 * *run_time_keys: receives ptr to array of IndexRuntimeKeyInfos, or NULL if none
 * *num_run_time_keys: receives number of runtime keys
 * *array_keys: receives ptr to array of IndexArrayKeyInfos, or NULL if none
 * *num_array_keys: receives number of array keys
 *
 * Caller may pass NULL for array_keys and num_array_keys to indicate that
 * IndexArrayKeyInfos are not supported.
 *		从索引条件表达式构建索引扫描键
 * 索引条件被以ScanKey数组的形式传递给索引AM。
 * 这个函数设置ScanKeys，填充ScanKeys的所有常量字段，并准备有关非常量比较值的键的信息。
 * 我们将索引条件表达式分为五种类型：
 *
 * 1. 简单操作符与常量比较值（"indexkey op constant"）。
 * 对于这些，我们只填充包含常量值的ScanKey。
 *
 * 2. 简单操作符与非常量值（"indexkey op expression"）。
 * 对于这些，我们创建一个ScanKey，除了表达式值以外的所有内容都填充好，并设置一个IndexRuntimeKeyInfo结构来驱动表达式在正确的时间进行评估。
 *
 * 3. RowCompareExpr（"(indexkey, indexkey, ...) op (expr, expr, ...)"）。
 * 对于这些，我们创建一个头部ScanKey和一个子ScanKey数组，如access/skey.h中所指定的。行比较的元素可以有常量或非常量比较值。
 *
 * 4. ScalarArrayOpExpr（"indexkey op ANY (array-expression)"）。如果索引有rd_am->amsearcharray
 * 我们将这些处理为简单操作符，设置SK_SEARCHARRAY标志告诉AM处理它们。否则，我们创建一个ScanKey，除了比较值以外的所有内容都填充好
 * 并设置一个IndexArrayKeyInfo结构来驱动qual的处理。
 * （注意，如果我们使用IndexArrayKeyInfo结构，数组表达式总是被视为需要运行时评估，即使它是一个常量。）
 *
 * 5. NullTest（"indexkey IS NULL/IS NOT NULL"）。我们只需正确填充ScanKey。
 *
 * 这段代码也用于为amcanorderbyop索引准备ORDER BY表达式。行为完全相同，只是我们必须以不同的方式查找操作符。注意，目前只有情况1和2适用于ORDER BY。
 *
 * 输入参数是：
 *
 * plan_state: 我们正在为之工作的执行器状态节点
 * index: 我们正在为之构建扫描键的索引
 * quals: 索引条件（或indexorderbys）表达式
 * is_order_by: 如果处理ORDER BY表达式，则为true；如果处理条件，则为false
 * *run_time_keys: 指向预先存在的IndexRuntimeKeyInfos的指针，如果没有则为NULL
 * *num_run_time_keys: 预先存在的运行时键的数量
 *
 * 输出参数是：
 *
 * scan_keys: 接收ScanKeys数组的指针
 * num_scan_keys: 接收扫描键的数量
 * run_time_keys: 接收IndexRuntimeKeyInfos数组的指针，如果没有则为NULL
 * num_run_time_keys: 接收运行时键的数量
 * array_keys: 接收IndexArrayKeyInfos数组的指针，如果没有则为NULL
 * num_array_keys: 接收数组键的数量
 *
 * 调用者可以传递NULL给array_keys和num_array_keys，表示不支持IndexArrayKeyInfos。
 */
void ExecIndexBuildScanKeys(PlanState* plan_state, Relation index, List* quals, bool is_order_by, ScanKey* scankeys,
    int* num_scan_keys, IndexRuntimeKeyInfo** run_time_keys, int* num_run_time_keys, IndexArrayKeyInfo** arraykeys,
    int* num_array_keys)
{
    ListCell* qual_cell = NULL;
    ScanKey scan_keys;
    IndexRuntimeKeyInfo* runtime_keys = NULL;
    IndexArrayKeyInfo* array_keys = NULL;
    int n_scan_keys;
    int n_runtime_keys;
    int max_runtime_keys;
    int n_array_keys;
    int j;

    /* Allocate array for ScanKey structs: one per qual 
    为ScanKey结构体数组分配内存：每个qual一个*/
    n_scan_keys = list_length(quals);
    scan_keys = (ScanKey)palloc(n_scan_keys * sizeof(ScanKeyData));

    /*
     * runtime_keys array is dynamically resized as needed.  We handle it this
     * way so that the same runtime keys array can be shared between
     * indexquals and indexorderbys, which will be processed in separate calls
     * of this function.  Caller must be sure to pass in NULL/0 for first
     * call.
    * runtime_keys数组根据需要动态调整大小。我们这样处理是为了让
    * indexquals和indexorderbys可以在此函数的不同调用中共享相同的runtime keys数组。
    * 调用者必须确保在第一次调用时传入NULL/0。
     */
    runtime_keys = *run_time_keys;
    n_runtime_keys = max_runtime_keys = *num_run_time_keys;

    /* Allocate array_keys as large as it could possibly need to be */
    array_keys = (IndexArrayKeyInfo*)palloc0(n_scan_keys * sizeof(IndexArrayKeyInfo));
    n_array_keys = 0;

    /*
     * for each opclause in the given qual, convert the opclause into a single
     * scan key
     * 
     * 对于给定qual中的每个opclause，将opclause转换为单个扫描键
     */
    j = 0;
    foreach (qual_cell, quals) {
        Expr* clause = (Expr*)lfirst(qual_cell);
        ScanKey this_scan_key = &scan_keys[j++];
        Oid opno;              /* operator's OID */
        RegProcedure opfuncid; /* operator proc id used in scan    reg:registered types*/
        Oid opfamily;          /* opfamily of index column */
        int op_strategy;       /* operator's strategy number */
        Oid op_lefttype;       /* operator's declared input types */
        Oid op_righttype;
        Expr* leftop = NULL;  /* expr on lhs of operator */
        Expr* rightop = NULL; /* expr on rhs(right hand side) ... */
        AttrNumber varattno;  /* att number used in scan */
        int indnkeyatts;
        // 获取索引的键属性数
        indnkeyatts = IndexRelationGetNumberOfKeyAttributes(index);
        if (IsA(clause, OpExpr)) {
            /* indexkey op const or indexkey op expression */
            uint32 flags = 0;
            Datum scan_value;

            opno = ((OpExpr*)clause)->opno;
            opfuncid = ((OpExpr*)clause)->opfuncid;

            /*
             * leftop should be the index key Var, possibly relabeled
             */
            leftop = (Expr*)get_leftop(clause);
            if (leftop && IsA(leftop, RelabelType))
                leftop = ((RelabelType*)leftop)->arg;
            Assert(leftop != NULL);
            if (!(IsA(leftop, Var) && ((Var*)leftop)->varno == INDEX_VAR))
                ereport(ERROR,
                    (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("indexqual for OpExpr doesn't have key on left side")));
            //varieble attribute number
            varattno = ((Var*)leftop)->varattno;
            if (varattno < 1 || varattno > indnkeyatts)
                ereport(ERROR,
                    (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("bogus index qualification for OpExpr, attribute number is %d.", varattno)));

            /*
             * We have to look up the operator's strategy number.  This
             * provides a cross-check that the operator does match the index.
             */
            opfamily = index->rd_opfamily[varattno - 1];
            //rd:relation data
            get_op_opfamily_properties(opno, opfamily, is_order_by, &op_strategy, &op_lefttype, &op_righttype);
            //flags写入scan key的标志位
            if (is_order_by)
                flags |= SK_ORDER_BY;

            /*
             * rightop is the constant or variable comparison value
             */
            rightop = (Expr*)get_rightop(clause);
            if (rightop && IsA(rightop, RelabelType))
            //relabletype:重新标记类型dummy node for type casts
                rightop = ((RelabelType*)rightop)->arg;
            Assert(rightop != NULL);
            if (IsA(rightop, Const)) {
                /* OK, simple constant comparison value */
                scan_value = ((Const*)rightop)->constvalue;
                if (((Const*)rightop)->constisnull)
                    flags |= SK_ISNULL;
            } else {
                /* Need to treat this one as a runtime key */
                if (n_runtime_keys >= max_runtime_keys) {
                    if (max_runtime_keys == 0) {
                        max_runtime_keys = 8;
                        runtime_keys = (IndexRuntimeKeyInfo*)palloc(max_runtime_keys * sizeof(IndexRuntimeKeyInfo));
                    } else {
                        max_runtime_keys *= 2;
                        runtime_keys = (IndexRuntimeKeyInfo*)repalloc(
                            runtime_keys, max_runtime_keys * sizeof(IndexRuntimeKeyInfo));
                    }
                }
                runtime_keys[n_runtime_keys].scan_key = this_scan_key;
                runtime_keys[n_runtime_keys].key_expr = ExecInitExpr(rightop, plan_state);
                runtime_keys[n_runtime_keys].key_toastable = TypeIsToastable(op_righttype);
                n_runtime_keys++;
                scan_value = (Datum)0;
            }

            /*
             * initialize the scan key's fields appropriately
             */
            ScanKeyEntryInitialize(this_scan_key,
                flags,
                varattno,                       /* attribute number to scan */
                op_strategy,                    /* op's strategy */
                op_righttype,                   /* strategy subtype */
                ((OpExpr*)clause)->inputcollid, /* collation */
                opfuncid,                       /* reg proc to use */
                scan_value);                     /* constant */
        } else if (IsA(clause, RowCompareExpr)) {
            /* (indexkey, indexkey, ...) op (expression, expression, ...) */
            //(SELECT column1, column2) < (SELECT columnA, columnB)
            RowCompareExpr* rc = (RowCompareExpr*)clause;
            ListCell* largs_cell = list_head(rc->largs);
            ListCell* rargs_cell = list_head(rc->rargs);
            ListCell* opnos_cell = list_head(rc->opnos);
            ListCell* collids_cell = list_head(rc->inputcollids);//OID list of collations for comparisons
            ScanKey first_sub_key;
            int n_sub_key;

            Assert(!is_order_by);

            first_sub_key = (ScanKey)palloc(list_length(rc->opnos) * sizeof(ScanKeyData));
            n_sub_key = 0;

            /* Scan RowCompare columns and generate subsidiary附属 ScanKey items */
            while (opnos_cell != NULL) {
                ScanKey this_sub_key = &first_sub_key[n_sub_key];
                int flags = SK_ROW_MEMBER;
                Datum scan_value;
                Oid inputcollation;

                /*
                 * leftop should be the index key Var, possibly relabeled
                 */
                leftop = (Expr*)lfirst(largs_cell);
                largs_cell = lnext(largs_cell);

                if (leftop && IsA(leftop, RelabelType))
                    leftop = ((RelabelType*)leftop)->arg;

                Assert(leftop != NULL);

                if (!(IsA(leftop, Var) && ((Var*)leftop)->varno == INDEX_VAR))
                    ereport(ERROR,
                        (errcode(ERRCODE_INDEX_CORRUPTED),
                            errmsg("indexqual for RowCompare expression doesn't have key on left side")));

                varattno = ((Var*)leftop)->varattno;

                /*
                 * We have to look up the operator's associated btree support
                 * function
                 */
                opno = lfirst_oid(opnos_cell);
                opnos_cell = lnext(opnos_cell);
                //varattno 不能越界
                if (!OID_IS_BTREE(index->rd_rel->relam) || varattno < 1 || varattno > index->rd_index->indnatts)
                    ereport(ERROR,
                        (errcode(ERRCODE_INDEX_CORRUPTED),
                            errmsg("bogus RowCompare index qualification, attribute number is %d", varattno)));
                opfamily = index->rd_opfamily[varattno - 1];

                get_op_opfamily_properties(opno, opfamily, is_order_by, &op_strategy, &op_lefttype, &op_righttype);
                 //LT LE GE or GT触发模式？
                if (op_strategy != rc->rctype)
                    ereport(ERROR,
                        (errcode(ERRCODE_INDEX_CORRUPTED),
                            errmsg("RowCompare index qualification contains wrong operator, strategy number is %d.",
                                op_strategy)));

                opfuncid = get_opfamily_proc(opfamily, op_lefttype, op_righttype, BTORDER_PROC);

                inputcollation = lfirst_oid(collids_cell);
                collids_cell = lnext(collids_cell);

                /*
                 * rightop is the constant or variable comparison value
                 */
                rightop = (Expr*)lfirst(rargs_cell);
                rargs_cell = lnext(rargs_cell);

                if (rightop && IsA(rightop, RelabelType))
                    rightop = ((RelabelType*)rightop)->arg;

                Assert(rightop != NULL);

                if (IsA(rightop, Const)) {
                    /* OK, simple constant comparison value */
                    scan_value = ((Const*)rightop)->constvalue;
                    if (((Const*)rightop)->constisnull)
                        flags |= SK_ISNULL;
                } else {
                    /* Need to treat this one as a runtime key */
                    if (n_runtime_keys >= max_runtime_keys) {
                        if (max_runtime_keys == 0) {
                            max_runtime_keys = 8;
                            runtime_keys = (IndexRuntimeKeyInfo*)palloc(max_runtime_keys * sizeof(IndexRuntimeKeyInfo));
                        } else {
                            max_runtime_keys *= 2;
                            runtime_keys = (IndexRuntimeKeyInfo*)repalloc(
                                runtime_keys, max_runtime_keys * sizeof(IndexRuntimeKeyInfo));
                        }
                    }
                    runtime_keys[n_runtime_keys].scan_key = this_sub_key;
                    runtime_keys[n_runtime_keys].key_expr = ExecInitExpr(rightop, plan_state);
                    runtime_keys[n_runtime_keys].key_toastable = TypeIsToastable(op_righttype);
                    n_runtime_keys++;
                    scan_value = (Datum)0;
                }

                /*
                 * initialize the subsidiary scan key's fields appropriately
                 */
                ScanKeyEntryInitialize(this_sub_key,
                    flags,
                    varattno,       /* attribute number */
                    op_strategy,    /* op's strategy */
                    op_righttype,   /* strategy subtype */
                    inputcollation, /* collation */
                    opfuncid,       /* reg proc to use */
                    scan_value);     /* constant */
                n_sub_key++;
            }

            if (n_sub_key == 0) {
                ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("n_sub_key can not be zero")));
            }
            
            /* Mark the last subsidiary scankey correctly */
            first_sub_key[n_sub_key - 1].sk_flags |= SK_ROW_END;
            /*
             * We don't use ScanKeyEntryInitialize for the header because it
             * isn't going to contain a valid sk_func pointer.
             */
            errno_t errorno = memset_s(this_scan_key, sizeof(ScanKeyData), 0, sizeof(ScanKeyData));
            securec_check(errorno, "\0", "\0");
            this_scan_key->sk_flags = SK_ROW_HEADER;
            this_scan_key->sk_attno = first_sub_key->sk_attno;
            this_scan_key->sk_strategy = rc->rctype;
            /* sk_subtype, sk_collation, sk_func not used in a header */
            this_scan_key->sk_argument = PointerGetDatum(first_sub_key);
        } else if (IsA(clause, ScalarArrayOpExpr)) {
            /* indexkey op ANY (array-expression) */
            ScalarArrayOpExpr* saop = (ScalarArrayOpExpr*)clause;
            int flags = 0;
            Datum scan_value;

            Assert(!is_order_by);

            Assert(saop->useOr);
            opno = saop->opno;
            opfuncid = saop->opfuncid;

            /*
             * leftop should be the index key Var, possibly relabeled
             */
            leftop = (Expr*)linitial(saop->args);
            if (leftop && IsA(leftop, RelabelType))
                leftop = ((RelabelType*)leftop)->arg;
            Assert(leftop != NULL);
            if (!(IsA(leftop, Var) && ((Var*)leftop)->varno == INDEX_VAR))
                ereport(ERROR,
                    (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("indexqual for ScalarArray doesn't have key on left side")));

            varattno = ((Var*)leftop)->varattno;
            if (varattno < 1 || varattno > indnkeyatts)
                ereport(ERROR,
                    (errcode(ERRCODE_INDEX_CORRUPTED),
                        errmsg("bogus index qualification for ScalarArray, attribute number is %d.", varattno)));

            /*
             * We have to look up the operator's strategy number.  This
             * provides a cross-check that the operator does match the index.
             */
            opfamily = index->rd_opfamily[varattno - 1];

            get_op_opfamily_properties(opno, opfamily, is_order_by, &op_strategy, &op_lefttype, &op_righttype);

            /*
             * rightop is the constant or variable array value
             */
            rightop = (Expr*)lsecond(saop->args);
            if (rightop && IsA(rightop, RelabelType))
                rightop = ((RelabelType*)rightop)->arg;
            Assert(rightop != NULL);

            if (index->rd_am->amsearcharray) {//can AM handle ScalarArrayOpExpr quals? 
                /* Index AM will handle this like a simple operator */
                flags |= SK_SEARCHARRAY;
                if (IsA(rightop, Const)) {
                    /* OK, simple constant comparison value */
                    scan_value = ((Const*)rightop)->constvalue;
                    if (((Const*)rightop)->constisnull)
                        flags |= SK_ISNULL;
                } else {
                    /* Need to treat this one as a runtime key */
                    if (n_runtime_keys >= max_runtime_keys) {
                        if (max_runtime_keys == 0) {
                            max_runtime_keys = 8;
                            runtime_keys = (IndexRuntimeKeyInfo*)palloc(max_runtime_keys * sizeof(IndexRuntimeKeyInfo));
                        } else {
                            max_runtime_keys *= 2;
                            runtime_keys = (IndexRuntimeKeyInfo*)repalloc(
                                runtime_keys, max_runtime_keys * sizeof(IndexRuntimeKeyInfo));
                        }
                    }
                    runtime_keys[n_runtime_keys].scan_key = this_scan_key;
                    runtime_keys[n_runtime_keys].key_expr = ExecInitExpr(rightop, plan_state);

                    /*
                     * Careful here: the runtime expression is not of
                     * op_righttype, but rather is an array of same; so
                     * TypeIsToastable() isn't helpful.  However, we can
                     * assume that all array types are toastable.
                     */
                    runtime_keys[n_runtime_keys].key_toastable = true;
                    n_runtime_keys++;
                    scan_value = (Datum)0;
                }
            } else {
                /* Executor has to expand the array value */
                array_keys[n_array_keys].scan_key = this_scan_key;
                array_keys[n_array_keys].array_expr = ExecInitExpr(rightop, plan_state);
                /* the remaining fields were zeroed by palloc0 */
                n_array_keys++;
                scan_value = (Datum)0;
            }

            /*
             * initialize the scan key's fields appropriately
             */
            ScanKeyEntryInitialize(this_scan_key,
                flags,
                varattno,          /* attribute number to scan */
                op_strategy,       /* op's strategy */
                op_righttype,      /* strategy subtype */
                saop->inputcollid, /* collation */
                opfuncid,          /* reg proc to use */
                scan_value);        /* constant */
        } else if (IsA(clause, NullTest)) {
            /* indexkey IS NULL or indexkey IS NOT NULL */
            NullTest* ntest = (NullTest*)clause;
            int flags;

            Assert(!is_order_by);

            /*
             * argument should be the index key Var, possibly relabeled
             */
            leftop = ntest->arg;

            if (leftop && IsA(leftop, RelabelType))
                leftop = ((RelabelType*)leftop)->arg;

            Assert(leftop != NULL);

            if (!(IsA(leftop, Var) && ((Var*)leftop)->varno == INDEX_VAR))
                ereport(ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("NullTest indexqual has wrong key")));

            varattno = ((Var*)leftop)->varattno;

            /*
             * initialize the scan key's fields appropriately
             */
            switch (ntest->nulltesttype) {
                case IS_NULL:
                    flags = SK_ISNULL | SK_SEARCHNULL;
                    break;
                case IS_NOT_NULL:
                    flags = SK_ISNULL | SK_SEARCHNOTNULL;
                    break;
                default:
                    ereport(ERROR,
                        (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                            errmsg("unrecognized nulltesttype: %d", (int)ntest->nulltesttype)));
                    flags = 0; /* keep compiler quiet */
                    break;
            }

            ScanKeyEntryInitialize(this_scan_key,
                flags,
                varattno,        /* attribute number to scan */
                InvalidStrategy, /* no strategy */
                InvalidOid,      /* no strategy subtype */
                InvalidOid,      /* no collation */
                InvalidOid,      /* no reg proc for this */
                (Datum)0);       /* constant */
        } else
            ereport(ERROR,
                (errcode(ERRCODE_UNRECOGNIZED_NODE_TYPE),
                    errmsg("unsupported indexqual type: %d", (int)nodeTag(clause))));
    }

    Assert(n_runtime_keys <= max_runtime_keys);

    /* Get rid of any unused arrays */
    if (n_array_keys == 0) {
        pfree_ext(array_keys);
        array_keys = NULL;
    }

    /*
     * Return info to our caller.把初始化后的数据还给输入参数
     */
    *scankeys = scan_keys;
    *num_scan_keys = n_scan_keys;
    *run_time_keys = runtime_keys;
    *num_run_time_keys = n_runtime_keys;
    if (arraykeys != NULL) {
        *arraykeys = array_keys;
        *num_array_keys = n_array_keys;
    } else if (n_array_keys != 0)
        ereport(
            ERROR, (errcode(ERRCODE_INDEX_CORRUPTED), errmsg("ScalarArrayOpExpr index qual found where not allowed")));
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: construt a dummy relation with the next partition and the partitiobed
 *			: table for the following Indexscan, and swith the scaning relation to
 *			: the dummy relation
 * Description	:
 * Input		:
 * Output	:
 * Notes		:
 */
static void ExecInitNextPartitionForIndexScan(IndexScanState* node)
{
    Partition current_partition = NULL;
    Relation current_partition_rel = NULL;
    Partition current_index_partition = NULL;
    Relation current_index_partition_rel = NULL;
    IndexScan* plan = NULL;
    int param_no = -1;
    ParamExecData* param = NULL;
    int subPartParamno = -1;
    ParamExecData* SubPrtParam = NULL;

    IndexScanState* indexState = node;
    IndexScan *indexScan = (IndexScan *)node->ss.ps.plan;
    Snapshot scanSnap;    // 选择扫描快照
    scanSnap = TvChooseScanSnap(indexState->iss_RelationDesc, &indexScan->scan, &indexState->ss);
    //Tv timecapsule version
    plan = (IndexScan*)(node->ss.ps.plan);

    /* get partition sequnce */
    /* 获取分区序列 */
    param_no = plan->scan.plan.paramno;
    param = &(node->ss.ps.state->es_param_exec_vals[param_no]);
    node->ss.currentSlot = (int)param->value;

    subPartParamno = plan->scan.plan.subparamno;
    SubPrtParam = &(node->ss.ps.state->es_param_exec_vals[subPartParamno]);//paramter for subpartition

    Oid heapOid = node->iss_RelationDesc->rd_index->indrelid;
    //indrelid index relation id 
    Relation heapRelation = heap_open(heapOid, AccessShareLock);

    /* no heap scan here */
    node->ss.ss_currentScanDesc = NULL;
    /* 构造一个带有下一个索引分区的虚拟关系 */
    /* construct a dummy relation with the next index partition */
    if (RelationIsSubPartitioned(heapRelation)) {
        Partition subOnePart = (Partition)list_nth(node->ss.partitions, node->ss.currentSlot);
        List *subPartList = (List *)list_nth(node->ss.subpartitions, node->ss.currentSlot);
        List *subIndexList = (List *)list_nth(node->iss_IndexPartitionList,
                                              node->ss.currentSlot);
        current_partition = (Partition)list_nth(subPartList, (int)SubPrtParam->value);

        Relation subOnePartRel = partitionGetRelation(node->ss.ss_currentRelation, subOnePart);
         // 获取当前分区和当前索引分区
        current_partition_rel = partitionGetRelation(subOnePartRel, current_partition);
        current_index_partition = (Partition)list_nth(subIndexList, (int)SubPrtParam->value);
        releaseDummyRelation(&subOnePartRel);
    } else {
         // 如果关系不是子分区的，则直接获取当前分区和当前索引分区
        current_partition = (Partition)list_nth(node->ss.partitions, node->ss.currentSlot);
        current_partition_rel = partitionGetRelation(node->ss.ss_currentRelation, current_partition);
        current_index_partition = (Partition)list_nth(node->iss_IndexPartitionList, node->ss.currentSlot);
    }

    current_index_partition_rel = partitionGetRelation(node->iss_RelationDesc, current_index_partition);

    Assert(PointerIsValid(node->iss_CurrentIndexPartition));
    releaseDummyRelation(&(node->iss_CurrentIndexPartition)); // 释放虚拟关系并更新当前索引分区
    node->iss_CurrentIndexPartition = current_index_partition_rel;

    /* update scan-related partition */
    releaseDummyRelation(&(node->ss.ss_currentPartition));
    node->ss.ss_currentPartition = current_partition_rel;

    /* Initialize scan descriptor. */
    node->iss_ScanDesc = scan_handler_idx_beginscan(node->ss.ss_currentPartition,
        node->iss_CurrentIndexPartition,
        scanSnap,
        node->iss_NumScanKeys,
        node->iss_NumOrderByKeys,
        (ScanState*)node);
     // 如果扫描描述符不为空，则重新扫描
    if (node->iss_ScanDesc != NULL) {
        scan_handler_idx_rescan_local(
            node->iss_ScanDesc, node->iss_ScanKeys, node->iss_NumScanKeys,
            node->iss_OrderByKeys, node->iss_NumOrderByKeys);
    }

    heap_close(heapRelation, AccessShareLock);
}

/*
 * @@GaussDB@@
 * Target		: data partition
 * Brief		: get index partitions list and table partitions list for the
 *			: the following IndexScan
 * Description	:
 * Input		:
 * Output	:
 * Notes		:
 */
void ExecInitPartitionForIndexScan(IndexScanState* index_state, EState* estate)
{
    IndexScan* plan = NULL;
    Relation current_relation = NULL;
    Partition table_partition = NULL;
    Partition index_partition = NULL;

    index_state->ss.partitions = NIL;
    index_state->ss.ss_currentPartition = NULL;
    index_state->iss_IndexPartitionList = NIL;
    index_state->iss_CurrentIndexPartition = NULL;
    // 获取计划和当前关系
    plan = (IndexScan*)index_state->ss.ps.plan;
    current_relation = index_state->ss.ss_currentRelation;

    if (plan->scan.itrs > 0) {
        Oid indexid = plan->indexid;
        bool relis_target = false;
        Partition indexpartition = NULL;
        LOCKMODE lock;

        /*
         * get relation's lockmode that hangs on whether
         * it's one of the target relations of the query
         * 获取关系的锁模式，这取决于它是否是查询的目标关系之一
         * relid is index into the range table
         */
        relis_target = ExecRelationIsTargetRelation(estate, plan->scan.scanrelid);
        lock = (relis_target ? RowExclusiveLock : AccessShareLock);
        index_state->ss.lockMode = lock;
        index_state->lockMode = lock;
        
        PruningResult* resultPlan = NULL;
         // 如果剪枝信息的表达式不为空
        if (plan->scan.pruningInfo->expr != NULL) {
            resultPlan = GetPartitionInfo(plan->scan.pruningInfo, estate, current_relation);
        } else {
            resultPlan = plan->scan.pruningInfo;
        }
        
        if (resultPlan->ls_rangeSelectedPartitions != NULL) {
            // 设置分区id为选中的分区范围的长度
            index_state->ss.part_id = resultPlan->ls_rangeSelectedPartitions->length;
        } else {
            index_state->ss.part_id = 0;
        }

        ListCell* cell1 = NULL;
        ListCell* cell2 = NULL;
        List* part_seqs = resultPlan->ls_rangeSelectedPartitions;
        List* partitionnos = resultPlan->ls_selectedPartitionnos;
        Assert(list_length(part_seqs) == list_length(partitionnos));
        StringInfo partNameInfo = makeStringInfo();
        StringInfo partOidInfo = makeStringInfo();
        // 遍历分区序列和分区号
        forboth (cell1, part_seqs, cell2, partitionnos) {
            Oid tablepartitionid = InvalidOid;
            Oid indexpartitionid = InvalidOid;
            List* partitionIndexOidList = NIL;
            int partSeq = lfirst_int(cell1);
            int partitionno = lfirst_int(cell2);

            /* get table partition and add it to a list for following scan */
            tablepartitionid = getPartitionOidFromSequence(current_relation, partSeq, partitionno);
            table_partition = PartitionOpenWithPartitionno(current_relation, tablepartitionid, partitionno, lock);
            index_state->ss.partitions = lappend(index_state->ss.partitions, table_partition);

            appendStringInfo(partNameInfo, "%s ", table_partition->pd_part->relname.data);
            appendStringInfo(partOidInfo, "%u ", tablepartitionid);
            // 如果当前关系是子分区
            if (RelationIsSubPartitioned(current_relation)) {
                ListCell *lc1 = NULL;
                ListCell *lc2 = NULL;
                SubPartitionPruningResult* subPartPruningResult =
                    GetSubPartitionPruningResult(resultPlan->ls_selectedSubPartitions, partSeq, partitionno);
                if (subPartPruningResult == NULL) {
                    continue;
                }
                List *subpartList = subPartPruningResult->ls_selectedSubPartitions;
                List *subpartitionnos = subPartPruningResult->ls_selectedSubPartitionnos;
                Assert(list_length(subpartList) == list_length(subpartitionnos));
                List *subIndexList = NULL;
                List *subRelationList = NULL;

                forboth (lc1, subpartList, lc2, subpartitionnos)
                {
                    int subpartSeq = lfirst_int(lc1);
                    int subpartitionno = lfirst_int(lc2);
                    Relation tablepartrel = partitionGetRelation(current_relation, table_partition);
                    Oid subpartitionid = getPartitionOidFromSequence(tablepartrel, subpartSeq, subpartitionno);
                    Partition subpart =
                        PartitionOpenWithPartitionno(tablepartrel, subpartitionid, subpartitionno, AccessShareLock);

                    partitionIndexOidList = PartitionGetPartIndexList(subpart);

                    Assert(partitionIndexOidList != NULL);
                    if (!PointerIsValid(partitionIndexOidList)) {
                        ereport(ERROR, (errmodule(MOD_OPT), errcode(ERRCODE_WRONG_OBJECT_TYPE),
                                        errmsg("no local indexes found for partition %s",
                                               PartitionGetPartitionName(subpart))));
                    }

                    indexpartitionid = searchPartitionIndexOid(indexid, partitionIndexOidList);
                    list_free_ext(partitionIndexOidList);
                    indexpartition = partitionOpen(index_state->iss_RelationDesc, indexpartitionid, AccessShareLock);

                    releaseDummyRelation(&tablepartrel);

                    if (indexpartition->pd_part->indisusable == false) {
                        ereport(
                            ERROR,
                            (errcode(ERRCODE_INDEX_CORRUPTED), errmodule(MOD_EXECUTOR),
                             errmsg(
                                 "can't initialize bitmap index scans using unusable local index \"%s\" for partition",
                                 PartitionGetPartitionName(indexpartition))));
                    }

                    subIndexList = lappend(subIndexList, indexpartition);
                    subRelationList = lappend(subRelationList, subpart);
                }

                index_state->iss_IndexPartitionList = lappend(index_state->iss_IndexPartitionList,
                                                              subIndexList);
                index_state->ss.subpartitions = lappend(index_state->ss.subpartitions, subRelationList);
                index_state->ss.subPartLengthList =
                    lappend_int(index_state->ss.subPartLengthList, list_length(subIndexList));
            } else {
                /* get index partition and add it to a list for following scan */
                // 获取索引分区并将其添加到后续扫描的列表中
                partitionIndexOidList = PartitionGetPartIndexList(table_partition);
                Assert(PointerIsValid(partitionIndexOidList));// 断言分区索引Oid列表是有效的
                if (!PointerIsValid(partitionIndexOidList)) {
                    ereport(ERROR,
                            (errcode(ERRCODE_WRONG_OBJECT_TYPE), errmsg("no local indexes found for partition %s",
                                                                        PartitionGetPartitionName(table_partition))));
                }
                indexpartitionid = searchPartitionIndexOid(indexid, partitionIndexOidList);
                list_free_ext(partitionIndexOidList);

                index_partition = partitionOpen(index_state->iss_RelationDesc, indexpartitionid, lock);
                if (index_partition->pd_part->indisusable == false) {// 如果索引分区不可用，报错
                    ereport(ERROR,
                                (errcode(ERRCODE_INDEX_CORRUPTED),
                                 errmsg("can't initialize index scans using unusable local index \"%s\"",
                                     PartitionGetPartitionName(index_partition))));
                }
                index_state->iss_IndexPartitionList = lappend(index_state->iss_IndexPartitionList, index_partition);
            }
        }
    }
}
